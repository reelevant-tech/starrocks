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

import com.starrocks.common.Config;
import com.starrocks.planner.MetaScanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.Backend;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class PreferReplicaLocationTest extends SchedulerTestBase {
    private static final long BACKEND2_ID = 10002L;
    private static final long BACKEND3_ID = 10003L;
    private static final Set<Long> LOCATION_A = Set.of(BACKEND1_ID);
    private static final Set<Long> LOCATION_B = Set.of(BACKEND2_ID, BACKEND3_ID);
    private static final Set<Long> ALL_BACKENDS = Set.of(BACKEND1_ID, BACKEND2_ID, BACKEND3_ID);
    private static final int RUNS = 20;

    private static final String SCAN = "select * from t_prefer_location";
    // Rewritten to a MetaScanNode, which lists its replicas apart from OlapScanNode.
    private static final String COUNT = "select count(*) from t_prefer_location";
    // Group by a column that is not the distribution key: scan, shuffled aggregation and gather fragments.
    private static final String SHUFFLE_AGG = "select v1, count(*) from t_prefer_location group by v1";

    @BeforeAll
    public static void beforeClass() throws Exception {
        SchedulerTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE t_prefer_location (k1 INT, v1 BIGINT) DUPLICATE KEY(k1) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 8 PROPERTIES (\"replication_num\" = \"3\")");
        setBackendLocation(BACKEND1_ID, Map.of("dc", "a"));
        setBackendLocation(BACKEND2_ID, Map.of("dc", "b"));
        setBackendLocation(BACKEND3_ID, Map.of("dc", "b"));
    }

    @AfterAll
    public static void afterClass() {
        for (long backendId : ALL_BACKENDS) {
            setBackendLocation(backendId, new HashMap<>());
        }
        SchedulerTestBase.afterClass();
    }

    @AfterEach
    public void afterEach() {
        connectContext.getSessionVariable().setPreferReplicaLocation("");
        Config.default_prefer_replica_location = "";
    }

    private static void setBackendLocation(long backendId, Map<String, String> location) {
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(backendId).setLocation(location);
    }

    private Map<String, Set<Long>> scheduledWorkers(String sql) throws Exception {
        DefaultCoordinator coordinator = startScheduling(sql);
        Map<String, Set<Long>> workerIdsByFragment = new TreeMap<>();
        List<ExecutionFragment> fragments = coordinator.getExecutionDAG().getFragmentsInPreorder();
        Assertions.assertTrue(fragments.size() > 1, "the query should have a gather fragment above the scan");
        for (ExecutionFragment fragment : fragments) {
            String name = fragment.getFragmentId() + (fragment.getPlanFragment().isGatherFragment() ? " (gather)" : "");
            for (FragmentInstance instance : fragment.getInstances()) {
                workerIdsByFragment.computeIfAbsent(name, k -> new TreeSet<>()).add(instance.getWorkerId());
            }
        }
        return workerIdsByFragment;
    }

    private static Set<Long> allWorkers(Map<String, Set<Long>> workerIdsByFragment) {
        Set<Long> workerIds = new HashSet<>();
        workerIdsByFragment.values().forEach(workerIds::addAll);
        return workerIds;
    }

    /**
     * @return the workers that scan tablets: with 8 tablets, every backend that may scan does scan
     */
    private Set<Long> scanWorkers(String sql) throws Exception {
        DefaultCoordinator coordinator = startScheduling(sql);
        Set<Long> workerIds = new HashSet<>();
        for (ExecutionFragment fragment : coordinator.getExecutionDAG().getFragmentsInPreorder()) {
            for (FragmentInstance instance : fragment.getInstances()) {
                if (!instance.getNode2ScanRanges().isEmpty()) {
                    workerIds.add(instance.getWorkerId());
                }
            }
        }
        return workerIds;
    }

    @Test
    public void testScanOnlyReadsReplicasInPreferredLocation() throws Exception {
        connectContext.getSessionVariable().setPreferReplicaLocation("dc:b");
        Assertions.assertEquals(LOCATION_B, scanWorkers(SCAN));
    }

    @Test
    public void testMetaScanOnlyReadsReplicasInPreferredLocation() throws Exception {
        connectContext.getSessionVariable().setPreferReplicaLocation("dc:b");
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, COUNT).second;
        Assertions.assertInstanceOf(MetaScanNode.class, plan.getScanNodes().get(0));
        Assertions.assertEquals(LOCATION_B, scanWorkers(COUNT));
    }

    @Test
    public void testScanReadsAllReplicasWithoutPreferredLocation() throws Exception {
        Assertions.assertEquals(ALL_BACKENDS, scanWorkers(SCAN));
        Assertions.assertEquals(ALL_BACKENDS, scanWorkers(COUNT));
    }

    @Test
    public void testScanFallsBackWhenNoReplicaInPreferredLocation() throws Exception {
        connectContext.getSessionVariable().setPreferReplicaLocation("dc:c");
        Assertions.assertEquals(ALL_BACKENDS, scanWorkers(SCAN));
        Assertions.assertEquals(ALL_BACKENDS, scanWorkers(COUNT));
    }

    @Test
    public void testSeveralPreferredLocations() throws Exception {
        connectContext.getSessionVariable().setPreferReplicaLocation("dc:a, dc:b");
        Assertions.assertEquals(ALL_BACKENDS, scanWorkers(SCAN));

        connectContext.getSessionVariable().setPreferReplicaLocation("dc:*");
        Assertions.assertEquals(ALL_BACKENDS, scanWorkers(SCAN));
    }

    @Test
    public void testFrontendConfigIsTheDefault() throws Exception {
        Config.default_prefer_replica_location = "dc:a";
        Assertions.assertEquals(LOCATION_A, scanWorkers(SCAN));

        connectContext.getSessionVariable().setPreferReplicaLocation("dc:b");
        Assertions.assertEquals(LOCATION_B, scanWorkers(SCAN));
    }

    @Test
    public void testMalformedFrontendConfigIsIgnored() throws Exception {
        Config.default_prefer_replica_location = "not a location";
        Assertions.assertEquals(ALL_BACKENDS, scanWorkers(SCAN));
    }

    @Test
    public void testWholeQueryStaysInPreferredLocation() throws Exception {
        for (String sql : List.of(COUNT, SHUFFLE_AGG)) {
            connectContext.getSessionVariable().setPreferReplicaLocation("dc:b");
            for (int i = 0; i < RUNS; i++) {
                Map<String, Set<Long>> workers = scheduledWorkers(sql);
                Assertions.assertTrue(LOCATION_B.containsAll(allWorkers(workers)), sql + ", workers outside dc:b: " + workers);
            }

            connectContext.getSessionVariable().setPreferReplicaLocation("dc:a");
            for (int i = 0; i < RUNS; i++) {
                Map<String, Set<Long>> workers = scheduledWorkers(sql);
                Assertions.assertEquals(LOCATION_A, allWorkers(workers), sql + ", workers outside dc:a: " + workers);
            }
        }
    }

    @Test
    public void testGatherFragmentFallsBackWithoutWorkerInPreferredLocation() throws Exception {
        connectContext.getSessionVariable().setPreferReplicaLocation("dc:c");
        for (int i = 0; i < RUNS; i++) {
            Assertions.assertTrue(ALL_BACKENDS.containsAll(allWorkers(scheduledWorkers(SHUFFLE_AGG))));
        }
    }

    @Test
    public void testDeadPreferredLocationFallsBackToOtherReplicas() throws Exception {
        Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(BACKEND1_ID);
        backend.setAlive(false);
        try {
            // The only backend of dc:a is down: read the other replicas instead of failing the query.
            connectContext.getSessionVariable().setPreferReplicaLocation("dc:a");
            Assertions.assertEquals(LOCATION_B, scanWorkers(SCAN));
            for (String sql : List.of(COUNT, SHUFFLE_AGG)) {
                for (int i = 0; i < RUNS; i++) {
                    Map<String, Set<Long>> workers = scheduledWorkers(sql);
                    Assertions.assertTrue(LOCATION_B.containsAll(allWorkers(workers)), sql + ", workers: " + workers);
                }
            }
        } finally {
            backend.setAlive(true);
        }
    }

    @Test
    public void testDeadBackendIsLeftOutOfPreferredLocation() throws Exception {
        Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(BACKEND2_ID);
        backend.setAlive(false);
        try {
            connectContext.getSessionVariable().setPreferReplicaLocation("dc:b");
            Assertions.assertEquals(Set.of(BACKEND3_ID), scanWorkers(SCAN));
        } finally {
            backend.setAlive(true);
        }
    }

    @Test
    public void testBlocklistedPreferredLocationFallsBackToOtherReplicas() throws Exception {
        SimpleScheduler.addToBlocklist(BACKEND1_ID);
        try {
            connectContext.getSessionVariable().setPreferReplicaLocation("dc:a");
            Assertions.assertEquals(LOCATION_B, scanWorkers(SCAN));
        } finally {
            SimpleScheduler.getHostBlacklist().clear();
        }
    }

    @Test
    public void testOrderedTiersFallBackToTheNextTier() throws Exception {
        connectContext.getSessionVariable().setPreferReplicaLocation("dc:a; dc:b");
        Assertions.assertEquals(LOCATION_A, scanWorkers(SCAN));

        Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(BACKEND1_ID);
        backend.setAlive(false);
        try {
            // The whole query moves to the next tier, gather fragment included.
            Assertions.assertEquals(LOCATION_B, scanWorkers(SCAN));
            for (int i = 0; i < RUNS; i++) {
                Map<String, Set<Long>> workers = scheduledWorkers(SHUFFLE_AGG);
                Assertions.assertTrue(LOCATION_B.containsAll(allWorkers(workers)), "workers outside dc:b: " + workers);
            }
        } finally {
            backend.setAlive(true);
        }
    }

    @Test
    public void testAutoFollowsTheMeasuredRoundTrip() throws Exception {
        BackendLatencyProber prober = GlobalStateMgr.getCurrentState().getBackendLatencyProber();
        connectContext.getSessionVariable().setPreferReplicaLocation("auto");
        try {
            prober.clearForTest();
            // Nothing measured yet: no preference.
            Assertions.assertEquals(ALL_BACKENDS, scanWorkers(SCAN));

            prober.setRttMsForTest(BACKEND1_ID, 4.6);
            prober.setRttMsForTest(BACKEND2_ID, 0.1);
            prober.setRttMsForTest(BACKEND3_ID, 0.1);
            Assertions.assertEquals(LOCATION_B, scanWorkers(SCAN));
            for (int i = 0; i < RUNS; i++) {
                Map<String, Set<Long>> workers = scheduledWorkers(SHUFFLE_AGG);
                Assertions.assertTrue(LOCATION_B.containsAll(allWorkers(workers)), "workers outside dc:b: " + workers);
            }

            prober.setRttMsForTest(BACKEND1_ID, 0.1);
            prober.setRttMsForTest(BACKEND2_ID, 4.6);
            prober.setRttMsForTest(BACKEND3_ID, 4.6);
            Assertions.assertEquals(LOCATION_A, scanWorkers(SCAN));
            Assertions.assertEquals(LOCATION_A, allWorkers(scheduledWorkers(SHUFFLE_AGG)));
        } finally {
            prober.clearForTest();
        }
    }
}
