// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.starrocks.clone.TabletChecker;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.BackendLatencyProber.LocationTier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * The locations preferred for the backends of a query, as ordered tiers: the session variable prefer_replica_location
 * lists them nearest first, or "auto" orders the backend locations by the round trip measured by
 * {@link BackendLatencyProber}. Before the backends are picked, {@link #prepare} chooses one tier for the whole query,
 * the first one that has an available replica of every tablet, else the one that has the most. The scans read the
 * replicas in that tier and the gather fragment runs there, so that the fragments exchange data inside one location;
 * a tablet without a replica there is read from the next tiers in order, then from any backend.
 */
public class ReplicaLocationPreference {
    private final List<String> tierNames;
    private final List<Set<Long>> tiers;
    private int chosenTier = 0;
    private int tabletCount = 0;
    private int coveredCount = 0;

    @VisibleForTesting
    ReplicaLocationPreference(List<String> tierNames, List<Set<Long>> tiers) {
        this.tierNames = tierNames;
        this.tiers = tiers;
    }

    /**
     * @return null when the session prefers no location, in shared-data mode where compute nodes have no location, or
     * in auto mode before any backend is measured
     */
    public static ReplicaLocationPreference of(ConnectContext context) {
        if (context == null || RunMode.isSharedDataMode()) {
            return null;
        }
        SessionVariable sessionVariable = context.getSessionVariable();
        if (sessionVariable.isPreferReplicaLocationAuto()) {
            List<LocationTier> measured = GlobalStateMgr.getCurrentState().getBackendLatencyProber().getTiers();
            if (measured.isEmpty() || Double.isNaN(measured.get(0).rttMs)) {
                return null;
            }
            return new ReplicaLocationPreference(
                    measured.stream().map(tier -> tier.location).collect(Collectors.toList()),
                    measured.stream().map(tier -> tier.backendIds).collect(Collectors.toList()));
        }
        List<String> locations = sessionVariable.getPreferReplicaLocationTiers();
        if (locations == null) {
            return null;
        }
        List<Backend> backends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends();
        List<Set<Long>> tiers = new ArrayList<>();
        for (String location : locations) {
            Multimap<String, String> required = PropertyAnalyzer.analyzeLocationStringToMap(location);
            tiers.add(backends.stream()
                    .filter(backend -> TabletChecker.isLocationMatch(required, backend.getSingleLevelLocationKV()))
                    .map(Backend::getId).collect(Collectors.toCollection(LinkedHashSet::new)));
        }
        return new ReplicaLocationPreference(locations, tiers);
    }

    /**
     * Choose the tier of the query from the replicas of its tablets: the first tier with an available replica of
     * every tablet, else the tier with the most, the first one on ties.
     */
    public void prepare(Collection<List<TScanRangeLocations>> scanRanges, Predicate<Long> isAvailable) {
        int total = 0;
        int[] covered = new int[tiers.size()];
        for (List<TScanRangeLocations> ranges : scanRanges) {
            for (TScanRangeLocations range : ranges) {
                total++;
                for (int tier = 0; tier < tiers.size(); tier++) {
                    for (TScanRangeLocation location : range.getLocations()) {
                        if (tiers.get(tier).contains(location.getBackend_id()) && isAvailable.test(location.getBackend_id())) {
                            covered[tier]++;
                            break;
                        }
                    }
                }
            }
        }
        chosenTier = 0;
        for (int tier = 0; tier < tiers.size(); tier++) {
            if (covered[tier] == total) {
                chosenTier = tier;
                break;
            }
            if (covered[tier] > covered[chosenTier]) {
                chosenTier = tier;
            }
        }
        tabletCount = total;
        coveredCount = tiers.isEmpty() ? 0 : covered[chosenTier];
        if (MetricRepo.hasInit && total > 0) {
            (chosenTier == 0 ? MetricRepo.COUNTER_REPLICA_LOCATION_FIRST_TIER : MetricRepo.COUNTER_REPLICA_LOCATION_LATER_TIER)
                    .increase(1L);
            if (isSplit()) {
                MetricRepo.COUNTER_REPLICA_LOCATION_SPLIT.increase(1L);
            }
        }
    }

    public String getChosenTierName() {
        return tierNames.isEmpty() ? "" : tierNames.get(chosenTier);
    }

    /**
     * @return whether some tablets have no available replica in the chosen tier, so that the query spans locations
     */
    public boolean isSplit() {
        return coveredCount < tabletCount;
    }

    /**
     * The order in which a tablet is read: the chosen tier, then the other tiers nearest first, then the backends
     * outside every tier.
     */
    private int rank(long backendId) {
        for (int tier = 0; tier < tiers.size(); tier++) {
            if (tiers.get(tier).contains(backendId)) {
                return tier == chosenTier ? -1 : tier;
            }
        }
        return tiers.size();
    }

    private <T> List<T> bestRanked(Collection<T> candidates, ToLongFunction<T> backendId, Predicate<Long> isAvailable) {
        int bestRank = Integer.MAX_VALUE;
        List<T> best = new ArrayList<>();
        for (T candidate : candidates) {
            long id = backendId.applyAsLong(candidate);
            if (!isAvailable.test(id)) {
                continue;
            }
            int rank = rank(id);
            if (rank < bestRank) {
                bestRank = rank;
                best.clear();
            }
            if (rank == bestRank) {
                best.add(candidate);
            }
        }
        return best;
    }

    /**
     * @return the given backends that are available, in the best tier among them, or all of them when none is
     * available, so that the caller reports the failure
     */
    public List<Long> preferBackends(Collection<Long> backendIds, Predicate<Long> isAvailable) {
        List<Long> best = bestRanked(backendIds, Long::longValue, isAvailable);
        return best.isEmpty() ? List.copyOf(backendIds) : best;
    }

    /**
     * @return the locations of a scan range whose backend is available, in the best tier among them, or all of them
     * when none is available
     */
    public List<TScanRangeLocation> preferLocations(List<TScanRangeLocation> all, Predicate<Long> isAvailable) {
        List<TScanRangeLocation> best = bestRanked(all, TScanRangeLocation::getBackend_id, isAvailable);
        return best.isEmpty() ? all : best;
    }

    public List<TScanRangeLocation> preferLocations(List<TScanRangeLocation> all, WorkerProvider workerProvider) {
        return preferLocations(all, workerProvider::isDataNodeAvailable);
    }

    /**
     * @return a worker in a tier, the best one first among the given workers, then among all the available workers;
     * empty when none is in a tier
     */
    public OptionalLong pickWorker(Collection<Long> workerIds, WorkerProvider workerProvider, Random random) {
        List<Long> candidates = bestRanked(new LinkedHashSet<>(workerIds), Long::longValue, id -> rank(id) < tiers.size());
        if (candidates.isEmpty()) {
            candidates = bestRanked(workerProvider.getAllAvailableNodes(), Long::longValue, id -> rank(id) < tiers.size());
        }
        if (candidates.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(candidates.get(random.nextInt(candidates.size())));
    }

    @Override
    public String toString() {
        return "tiers=" + tierNames + " chosen=" + getChosenTierName() + " covered=" + coveredCount + "/" + tabletCount;
    }
}
