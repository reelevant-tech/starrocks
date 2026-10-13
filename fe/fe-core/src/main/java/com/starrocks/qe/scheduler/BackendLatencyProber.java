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
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.metric.GaugeMetricImpl;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Measures, from this frontend, the network round trip to every backend (a TCP connection to its port, the minimum
 * over a window of samples) and orders the backend locations by it, nearest first. The session variable
 * prefer_replica_location set to "auto" reads the queries in the nearest location that has their replicas.
 */
public class BackendLatencyProber extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(BackendLatencyProber.class);
    private static final long UNREACHABLE_MICROS = Long.MAX_VALUE;
    public static final String NO_LOCATION = "";

    /**
     * The backends of one location, with the round trip from this frontend: the median of its backends, NaN when
     * none of them is measured.
     */
    public static class LocationTier {
        public final String location;
        public final Set<Long> backendIds;
        public final double rttMs;

        LocationTier(String location, Set<Long> backendIds, double rttMs) {
            this.location = location;
            this.backendIds = backendIds;
            this.rttMs = rttMs;
        }

        @Override
        public String toString() {
            return location + (Double.isNaN(rttMs) ? " (unmeasured)" : String.format(" (%.2f ms)", rttMs)) + " " + backendIds;
        }
    }

    private static class Window {
        private final long[] micros;
        private int next = 0;
        private int count = 0;

        Window(int size) {
            micros = new long[Math.max(1, size)];
        }

        synchronized void add(long value) {
            micros[next] = value;
            next = (next + 1) % micros.length;
            count = Math.min(count + 1, micros.length);
        }

        synchronized long min() {
            long min = UNREACHABLE_MICROS;
            for (int i = 0; i < count; i++) {
                min = Math.min(min, micros[i]);
            }
            return min;
        }
    }

    private final ExecutorService executor;
    private final ConcurrentMap<Long, Window> windows = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, Double> rttOverridesMs = new ConcurrentHashMap<>();
    private final Map<Long, GaugeMetricImpl<Double>> gauges = new HashMap<>();
    private volatile List<String> lastOrder = Collections.emptyList();

    public BackendLatencyProber() {
        super("backend-latency-prober", Config.backend_latency_probe_interval_ms);
        executor = ThreadPoolManager.newDaemonFixedThreadPool(4, 64, "backend-latency-prober", true);
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(Config.backend_latency_probe_interval_ms);
        List<Backend> backends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends();
        Map<Long, Future<Long>> futures = new HashMap<>();
        for (Backend backend : backends) {
            futures.put(backend.getId(), executor.submit(() -> connectMicros(backend.getHost(), backend.getBePort())));
        }
        for (Map.Entry<Long, Future<Long>> entry : futures.entrySet()) {
            long micros = UNREACHABLE_MICROS;
            try {
                micros = entry.getValue().get();
            } catch (Exception e) {
                LOG.warn("probing backend {} failed: {}", entry.getKey(), e.getMessage());
            }
            windows.computeIfAbsent(entry.getKey(), id -> new Window(Config.backend_latency_probe_window)).add(micros);
        }
        windows.keySet().retainAll(futures.keySet());
        updateGauges(backends);
        List<LocationTier> tiers = getTiers();
        List<String> order = tiers.stream().map(tier -> tier.location).collect(Collectors.toList());
        if (!order.equals(lastOrder)) {
            LOG.info("backend locations by round trip from this frontend: {}", tiers);
            lastOrder = order;
        }
    }

    private static long connectMicros(String host, int port) {
        long start = System.nanoTime();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), Config.backend_latency_probe_timeout_ms);
            return (System.nanoTime() - start) / 1000;
        } catch (IOException e) {
            return UNREACHABLE_MICROS;
        }
    }

    private void updateGauges(List<Backend> backends) {
        if (!MetricRepo.hasInit) {
            return;
        }
        for (Backend backend : backends) {
            GaugeMetricImpl<Double> gauge = gauges.get(backend.getId());
            if (gauge == null) {
                gauge = new GaugeMetricImpl<>("backend_rtt_ms", MetricUnit.MILLISECONDS,
                        "network round trip from this frontend to the backend, minimum over the probe window");
                gauge.addLabel(new MetricLabel("backend_id", String.valueOf(backend.getId())))
                        .addLabel(new MetricLabel("host", backend.getHost()));
                MetricRepo.addMetric(gauge);
                gauges.put(backend.getId(), gauge);
            }
            gauge.setValue(rttMs(backend.getId()).orElse(Double.NaN));
        }
    }

    /**
     * @return the round trip to the backend in milliseconds, empty when it was never reached in the window
     */
    public OptionalDouble rttMs(long backendId) {
        Double override = rttOverridesMs.get(backendId);
        if (override != null) {
            return OptionalDouble.of(override);
        }
        Window window = windows.get(backendId);
        if (window == null) {
            return OptionalDouble.empty();
        }
        long micros = window.min();
        return micros == UNREACHABLE_MICROS ? OptionalDouble.empty() : OptionalDouble.of(micros / 1000.0);
    }

    /**
     * @return the locations of the backends, nearest to this frontend first; the unmeasured ones last, in the order of
     * their names; backends without a location form their own tier, also last
     */
    public List<LocationTier> getTiers() {
        return tiersOf(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends());
    }

    @VisibleForTesting
    List<LocationTier> tiersOf(List<Backend> backends) {
        Map<String, Set<Long>> byLocation = new HashMap<>();
        for (Backend backend : backends) {
            Pair<String, String> location = backend.getSingleLevelLocationKV();
            String name = location == null ? NO_LOCATION : location.first + ":" + location.second;
            byLocation.computeIfAbsent(name, k -> new LinkedHashSet<>()).add(backend.getId());
        }
        List<LocationTier> tiers = new ArrayList<>();
        for (Map.Entry<String, Set<Long>> entry : byLocation.entrySet()) {
            List<Double> rtts = entry.getValue().stream()
                    .map(this::rttMs).filter(OptionalDouble::isPresent).map(OptionalDouble::getAsDouble)
                    .sorted().collect(Collectors.toList());
            double median = rtts.isEmpty() ? Double.NaN : rtts.get(rtts.size() / 2);
            tiers.add(new LocationTier(entry.getKey(), entry.getValue(), median));
        }
        tiers.sort(Comparator.comparing((LocationTier tier) -> Double.isNaN(tier.rttMs))
                .thenComparing(tier -> tier.location.equals(NO_LOCATION))
                .thenComparingDouble(tier -> tier.rttMs)
                .thenComparing(tier -> tier.location));
        return tiers;
    }

    @VisibleForTesting
    public void setRttMsForTest(long backendId, Double rttMs) {
        if (rttMs == null) {
            rttOverridesMs.remove(backendId);
        } else {
            rttOverridesMs.put(backendId, Objects.requireNonNull(rttMs));
        }
    }

    @VisibleForTesting
    public void clearForTest() {
        rttOverridesMs.clear();
        windows.clear();
    }
}
