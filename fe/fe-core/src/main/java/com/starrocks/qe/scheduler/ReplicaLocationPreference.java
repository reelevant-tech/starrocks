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

import com.google.common.collect.Multimap;
import com.starrocks.clone.TabletChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TScanRangeLocation;

import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The locations of the session variable prefer_replica_location, applied when the scheduler picks the backends of a
 * query: a scan reads the replicas on available backends in these locations and the gather fragment runs on one of
 * them, each falling back to the other backends when none is there.
 */
public class ReplicaLocationPreference {
    private final Multimap<String, String> locations;

    private ReplicaLocationPreference(Multimap<String, String> locations) {
        this.locations = locations;
    }

    /**
     * @return null when the session prefers no location, or in shared-data mode where compute nodes have no location
     */
    public static ReplicaLocationPreference of(ConnectContext context) {
        if (context == null || RunMode.isSharedDataMode()) {
            return null;
        }
        Multimap<String, String> locations = context.getSessionVariable().getPreferReplicaLocationMap();
        return locations == null ? null : new ReplicaLocationPreference(locations);
    }

    public boolean matches(long backendId) {
        return TabletChecker.isLocationMatch(backendId, locations);
    }

    /**
     * @return the given backends that are available and in the preferred locations, or all of them when none is, so
     * that a tablet without a replica there is read from its other replicas
     */
    public List<Long> preferBackends(Collection<Long> backendIds, Predicate<Long> isAvailable) {
        List<Long> preferred = backendIds.stream()
                .filter(id -> isAvailable.test(id) && matches(id))
                .collect(Collectors.toList());
        return preferred.isEmpty() ? List.copyOf(backendIds) : preferred;
    }

    /**
     * @return the locations of a scan range whose backend is available and in the preferred locations, or all of them
     * when none is
     */
    public List<TScanRangeLocation> preferLocations(List<TScanRangeLocation> all, WorkerProvider workerProvider) {
        List<TScanRangeLocation> preferred = all.stream()
                .filter(location -> workerProvider.isDataNodeAvailable(location.getBackend_id()) &&
                        matches(location.getBackend_id()))
                .collect(Collectors.toList());
        return preferred.isEmpty() ? all : preferred;
    }

    /**
     * @return a worker in the preferred locations, picked at random first among the given workers, then among all the
     * available workers; empty when none is there
     */
    public OptionalLong pickWorker(Collection<Long> workerIds, WorkerProvider workerProvider, Random random) {
        List<Long> candidates = workerIds.stream().distinct().filter(this::matches).collect(Collectors.toList());
        if (candidates.isEmpty()) {
            candidates = workerProvider.getAllAvailableNodes().stream().filter(this::matches).collect(Collectors.toList());
        }
        if (candidates.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(candidates.get(random.nextInt(candidates.size())));
    }
}
