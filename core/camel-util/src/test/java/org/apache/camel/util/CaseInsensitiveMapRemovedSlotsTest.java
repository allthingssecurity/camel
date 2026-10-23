/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A key that is removed leaves its slot in the arrays of the map; put must reuse those slots instead of growing the
 * arrays for ever, as headers that are set and removed in a loop on the same exchange do.
 */
class CaseInsensitiveMapRemovedSlotsTest {

    @Test
    void putAndRemoveOfTheSameKeyDoesNotGrowTheMap() {
        CaseInsensitiveMap map = new CaseInsensitiveMap();
        map.put("CamelFileName", "a.txt");
        for (int i = 0; i < 10000; i++) {
            map.put("tmp", i);
            assertEquals(i, map.get("TMP"));
            assertEquals(i, map.remove("Tmp"));
        }

        assertEquals(1, map.size());
        assertEquals("a.txt", map.get("camelFileName"));
        assertNull(map.get("tmp"));
        assertTrue(map.slotCapacity() <= 64, "the map holds " + map.slotCapacity() + " slots for 1 key");
    }

    @Test
    void everyWayOfRemovingAKeyFreesItsSlotForReuse() {
        Map<String, Consumer<Map<String, Object>>> removers = new LinkedHashMap<>();
        removers.put("remove(key)", m -> m.remove("TMP"));
        removers.put("remove(key, value)", m -> m.remove("TMP", "v"));
        removers.put("keySet().remove", m -> m.keySet().remove("tmp"));
        removers.put("keySet().removeAll", m -> m.keySet().removeAll(List.of("tmp")));
        removers.put("keySet().retainAll", m -> m.keySet().retainAll(List.of("Keep")));
        removers.put("keySet().removeIf", m -> m.keySet().removeIf("tmp"::equalsIgnoreCase));
        removers.put("values().remove", m -> m.values().remove("v"));
        removers.put("values().removeIf", m -> m.values().removeIf("v"::equals));
        removers.put("entrySet().remove", m -> m.entrySet().remove(Map.entry("tmp", "v")));
        removers.put("entrySet().removeIf", m -> m.entrySet().removeIf(e -> "v".equals(e.getValue())));
        removers.put("iterator().remove", m -> {
            for (Iterator<String> it = m.keySet().iterator(); it.hasNext();) {
                if ("tmp".equalsIgnoreCase(it.next())) {
                    it.remove();
                }
            }
        });
        removers.put("compute returning null", m -> m.compute("TMP", (k, v) -> null));
        removers.put("computeIfPresent returning null", m -> m.computeIfPresent("TMP", (k, v) -> null));
        removers.put("merge returning null", m -> m.merge("TMP", "w", (v, w) -> null));

        assertAll(removers.entrySet().stream().map(remover -> () -> {
            String name = remover.getKey();
            CaseInsensitiveMap map = new CaseInsensitiveMap();
            map.put("Keep", "k");
            for (int i = 0; i < 2000; i++) {
                map.put("tmp", "v");
                remover.getValue().accept(map);
                assertEquals(1, map.size(), name);
            }
            assertEquals("k", map.get("KEEP"), name);
            assertTrue(map.slotCapacity() <= 64, name + ": the map holds " + map.slotCapacity() + " slots for 1 key");
        }));
    }

    @Test
    void putAndRemoveNextToKeysJustBelowTheResizeThresholdDoesNotGrowTheMap() {
        CaseInsensitiveMap map = new CaseInsensitiveMap();
        // 11 keys, one below the threshold of the default table (16 * 0.75); "az" and "b[" share a hash code
        List<String> live = new ArrayList<>(List.of("az", "b["));
        for (int i = 0; i < 9; i++) {
            live.add("Live" + i);
        }
        live.forEach(k -> map.put(k, k));
        for (int i = 0; i < 10000; i++) {
            map.put("Tmp" + (i % 7), i);
            assertEquals(i, map.remove("TMP" + (i % 7)));
        }

        assertEquals(11, map.size());
        for (String k : live) {
            assertEquals(k, map.get(k.toUpperCase()));
        }
        assertTrue(map.slotCapacity() <= 64, "the map holds " + map.slotCapacity() + " slots for 11 keys");
    }

    @Test
    void removingAllKeysAgainAndAgainDoesNotGrowTheMap() {
        CaseInsensitiveMap map = new CaseInsensitiveMap();
        for (int round = 0; round < 1000; round++) {
            for (int k = 0; k < 10; k++) {
                map.put("Header" + k, round);
            }
            assertEquals(10, map.size());
            // as removeHeaders does: through the key set, and through an iterator
            if (round % 2 == 0) {
                map.keySet().removeAll(List.of("HEADER0", "HEADER1", "HEADER2", "HEADER3", "HEADER4"));
            }
            for (Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext();) {
                it.next();
                it.remove();
            }
            assertTrue(map.isEmpty());
        }

        assertTrue(map.slotCapacity() <= 64, "the map holds " + map.slotCapacity() + " slots for 0 keys");
    }

    @Test
    void keysAndTheirOrderAreKeptWhenRemovedSlotsAreReused() {
        CaseInsensitiveMap map = new CaseInsensitiveMap();
        for (int i = 0; i < 1000; i++) {
            map.put("key" + i, i);
        }
        for (int i = 0; i < 1000; i += 2) {
            map.remove("KEY" + i);
        }
        // grows and reuses the removed slots again while the odd keys stay
        for (int round = 0; round < 50; round++) {
            for (int i = 0; i < 600; i++) {
                map.put("other" + i, i);
            }
            for (int i = 0; i < 600; i++) {
                map.remove("OTHER" + i);
            }
        }
        map.put("Last", "x");

        assertEquals(501, map.size());
        for (int i = 0; i < 1000; i++) {
            assertEquals(i % 2 == 0 ? null : i, map.get("Key" + i));
        }
        assertEquals("x", map.get("last"));
        // insertion order
        List<String> expected = new ArrayList<>();
        for (int i = 1; i < 1000; i += 2) {
            expected.add("key" + i);
        }
        expected.add("Last");
        assertEquals(expected, new ArrayList<>(map.keySet()));
    }
}
