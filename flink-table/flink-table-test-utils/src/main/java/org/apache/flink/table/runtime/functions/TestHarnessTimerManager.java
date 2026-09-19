/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Timer and watermark manager for {@link ProcessTableFunctionTestHarness}.
 *
 * <p>Handles timer registration, clearing, watermark tracking, and determining which timers are
 * eligible to fire.
 */
@Internal
class TestHarnessTimerManager {

    private final Map<Row, Set<Timer>> pendingTimersByPartition = new HashMap<>();
    private final List<Timer> firedTimers = new ArrayList<>();
    private final Map<String, Long> watermarkByTable = new HashMap<>();
    @Nullable private Long globalWatermark;

    TestHarnessTimerManager() {}

    // -------------------------------------------------------------------------
    // Watermark
    // -------------------------------------------------------------------------

    void setTableWatermark(String tableName, long absoluteMillis) {
        Long current = watermarkByTable.get(tableName);
        if (current != null && absoluteMillis < current) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot move watermark backward for table '%s': current=%d, new=%d",
                            tableName, current, absoluteMillis));
        }
        watermarkByTable.put(tableName, absoluteMillis);
    }

    void updateGlobalWatermarkAndFireTimers(ThrowingConsumer<Timer, Exception> firer)
            throws Exception {
        if (watermarkByTable.isEmpty()) {
            return;
        }
        long newGlobalWatermark = Collections.min(watermarkByTable.values());
        if (globalWatermark != null && newGlobalWatermark < globalWatermark) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot move global watermark backward: current=%d, new=%d",
                            globalWatermark, newGlobalWatermark));
        }
        globalWatermark = newGlobalWatermark;
        fireEligibleTimers(newGlobalWatermark, firer);
    }

    @Nullable
    Long getGlobalWatermark() {
        return globalWatermark;
    }

    @Nullable
    Long getWatermarkForTable(String tableName) {
        return watermarkByTable.get(tableName);
    }

    // -------------------------------------------------------------------------
    // Timer Registration
    // -------------------------------------------------------------------------

    void register(Row partitionKey, long timestampMillis, @Nullable String name) {
        Set<Timer> timerSet =
                pendingTimersByPartition.computeIfAbsent(partitionKey, k -> new HashSet<>());

        if (name != null) {
            timerSet.removeIf(t -> name.equals(t.name));
        } else {
            timerSet.removeIf(t -> t.name == null && t.timestamp == timestampMillis);
        }
        timerSet.add(new Timer(timestampMillis, name, partitionKey));
    }

    void clearByName(Row partitionKey, String name) {
        Set<Timer> timerSet = pendingTimersByPartition.get(partitionKey);
        if (timerSet != null) {
            timerSet.removeIf(t -> name.equals(t.name));
        }
    }

    /** Clears unnamed timers matching the given timestamp. Named timers are not affected. */
    void clearByTimestamp(Row partitionKey, long timestamp) {
        Set<Timer> timerSet = pendingTimersByPartition.get(partitionKey);
        if (timerSet != null) {
            timerSet.removeIf(t -> t.name == null && t.timestamp == timestamp);
        }
    }

    void clearAll(Row partitionKey) {
        pendingTimersByPartition.remove(partitionKey);
    }

    // -------------------------------------------------------------------------
    // Timer Introspection
    // -------------------------------------------------------------------------

    List<Timer> getPendingTimers() {
        return pendingTimersByPartition.values().stream()
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.toList());
    }

    List<Timer> getFiredTimers() {
        return new ArrayList<>(firedTimers);
    }

    void clearFiredTimers() {
        firedTimers.clear();
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    // Loop until no more eligible timers — handles cascading registrations
    private void fireEligibleTimers(long watermark, ThrowingConsumer<Timer, Exception> firer)
            throws Exception {
        while (true) {
            List<Timer> timersToFire =
                    pendingTimersByPartition.values().stream()
                            .flatMap(Collection::stream)
                            .filter(t -> t.timestamp <= watermark)
                            .sorted()
                            .collect(Collectors.toList());

            if (timersToFire.isEmpty()) {
                break;
            }

            for (Timer timer : timersToFire) {
                Set<Timer> timerSet =
                        pendingTimersByPartition.getOrDefault(
                                timer.partitionKey, Collections.emptySet());

                if (timerSet.contains(timer)) {
                    timerSet.remove(timer);
                    timer.markFired();
                    firedTimers.add(timer);
                    firer.accept(timer);
                }
            }
        }
    }
}
