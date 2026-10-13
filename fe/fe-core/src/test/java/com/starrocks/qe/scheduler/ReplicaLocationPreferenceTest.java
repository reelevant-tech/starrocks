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

import com.starrocks.qe.scheduler.BackendLatencyProber.LocationTier;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The tier choice and the per-tablet fallback of {@link ReplicaLocationPreference}, on tiers a={1}, b={2,3}, c={4}
 * and backend 9 outside every tier.
 */
public class ReplicaLocationPreferenceTest {
    private static final Predicate<Long> ALL_AVAILABLE = id -> true;

    private static ReplicaLocationPreference tiers() {
        return new ReplicaLocationPreference(List.of("a", "b", "c"), List.of(Set.of(1L), Set.of(2L, 3L), Set.of(4L)));
    }

    private static TScanRangeLocations range(long... backendIds) {
        TScanRangeLocations range = new TScanRangeLocations();
        for (long backendId : backendIds) {
            TScanRangeLocation location = new TScanRangeLocation(new TNetworkAddress("127.0.0." + backendId, 9060));
            location.setBackend_id(backendId);
            range.addToLocations(location);
        }
        return range;
    }

    private static List<Long> backendIds(List<TScanRangeLocation> locations) {
        return locations.stream().map(TScanRangeLocation::getBackend_id).collect(Collectors.toList());
    }

    private static List<Long> preferred(ReplicaLocationPreference preference, TScanRangeLocations range,
                                        Predicate<Long> available) {
        return backendIds(preference.preferLocations(range.getLocations(), available));
    }

    @Test
    public void testFirstTierWithEveryTabletIsChosen() {
        ReplicaLocationPreference preference = tiers();
        preference.prepare(List.of(List.of(range(1, 2, 4), range(1, 3))), ALL_AVAILABLE);
        Assertions.assertEquals("a", preference.getChosenTierName());
        Assertions.assertFalse(preference.isSplit());
        Assertions.assertEquals(List.of(1L), preferred(preference, range(1, 2, 4), ALL_AVAILABLE));
        Assertions.assertEquals(List.of(1L), preference.preferBackends(List.of(4L, 2L, 1L), ALL_AVAILABLE));
    }

    @Test
    public void testNextTierWithEveryTabletIsChosenOverAPartialFirstTier() {
        ReplicaLocationPreference preference = tiers();
        preference.prepare(List.of(List.of(range(1, 2), range(2, 3))), ALL_AVAILABLE);
        Assertions.assertEquals("b", preference.getChosenTierName());
        Assertions.assertFalse(preference.isSplit());
        Assertions.assertEquals(List.of(2L), preferred(preference, range(1, 2), ALL_AVAILABLE));
        Assertions.assertEquals(List.of(2L, 3L), preferred(preference, range(2, 3), ALL_AVAILABLE));
    }

    @Test
    public void testSplitQueryReadsMissingTabletsFromTheNearestTier() {
        ReplicaLocationPreference preference = tiers();
        // Every tier covers one tablet of two: the first one is chosen, and the other tablets come from the next
        // tiers in order, then from backends outside every tier.
        preference.prepare(List.of(List.of(range(1, 4)), List.of(range(2, 9))), ALL_AVAILABLE);
        Assertions.assertEquals("a", preference.getChosenTierName());
        Assertions.assertTrue(preference.isSplit());
        Assertions.assertEquals(List.of(1L), preferred(preference, range(1, 4), ALL_AVAILABLE));
        Assertions.assertEquals(List.of(2L), preferred(preference, range(9, 2), ALL_AVAILABLE));
        Assertions.assertEquals(List.of(4L), preferred(preference, range(9, 4), ALL_AVAILABLE));
        Assertions.assertEquals(List.of(2L), preferred(preference, range(4, 2), ALL_AVAILABLE));
    }

    @Test
    public void testDeadBackendsDoNotCount() {
        ReplicaLocationPreference preference = tiers();
        Predicate<Long> available = id -> id != 1L;
        preference.prepare(List.of(List.of(range(1, 2))), available);
        Assertions.assertEquals("b", preference.getChosenTierName());
        Assertions.assertFalse(preference.isSplit());
        Assertions.assertEquals(List.of(2L), preferred(preference, range(1, 2), available));
        // With no available replica, every location is returned for the caller to report the failure.
        Assertions.assertEquals(List.of(1L), preferred(preference, range(1), available));
        Assertions.assertEquals(List.of(1L), preference.preferBackends(List.of(1L), available));
    }

    @Test
    public void testBackendsOutsideEveryTierComeLast() {
        ReplicaLocationPreference preference = tiers();
        preference.prepare(List.of(List.of(range(9))), ALL_AVAILABLE);
        Assertions.assertTrue(preference.isSplit());
        Assertions.assertEquals(List.of(9L), preferred(preference, range(9), ALL_AVAILABLE));
        Assertions.assertEquals(List.of(3L), preferred(preference, range(9, 3), ALL_AVAILABLE));
    }

    @Test
    public void testNoTabletKeepsTheFirstTier() {
        ReplicaLocationPreference preference = tiers();
        preference.prepare(List.of(), ALL_AVAILABLE);
        Assertions.assertEquals("a", preference.getChosenTierName());
        Assertions.assertFalse(preference.isSplit());
    }

    @Test
    public void testTiersOrderedByRoundTrip() {
        BackendLatencyProber prober = new BackendLatencyProber();
        List<Backend> backends = List.of(backend(1, "dc", "a"), backend(2, "dc", "b"), backend(3, "dc", "b"),
                backend(4, "dc", "c"), backend(9, null, null));
        prober.setRttMsForTest(1L, 4.6);
        prober.setRttMsForTest(2L, 0.1);
        prober.setRttMsForTest(3L, 5.0);
        prober.setRttMsForTest(4L, 1.8);

        // The round trip of a tier of two backends is the larger one, so that one far backend is not hidden: dc:b
        // ranks behind dc:c and dc:a despite its 0.1 ms backend.
        List<LocationTier> tiers = prober.tiersOf(backends);
        Assertions.assertEquals(List.of("dc:c", "dc:a", "dc:b", BackendLatencyProber.NO_LOCATION),
                tiers.stream().map(tier -> tier.location).collect(Collectors.toList()));
        Assertions.assertEquals(1.8, tiers.get(0).rttMs);
        Assertions.assertEquals(5.0, tiers.get(2).rttMs);
        Assertions.assertEquals(Set.of(2L, 3L), tiers.get(2).backendIds);
        Assertions.assertTrue(Double.isNaN(tiers.get(3).rttMs));

        prober.setRttMsForTest(1L, 0.05);
        Assertions.assertEquals("dc:a", prober.tiersOf(backends).get(0).location);
    }

    private static Backend backend(long id, String locationKey, String locationValue) {
        Backend backend = new Backend(id, "127.0.0." + id, 9050);
        if (locationKey != null) {
            backend.setLocation(Map.of(locationKey, locationValue));
        }
        return backend;
    }

    @Test
    public void testPreferredBackendsKeepTheirOrder() {
        ReplicaLocationPreference preference = tiers();
        preference.prepare(List.of(List.of(range(2, 3))), ALL_AVAILABLE);
        Assertions.assertEquals(Arrays.asList(3L, 2L), preference.preferBackends(List.of(3L, 2L, 9L), ALL_AVAILABLE));
    }
}
