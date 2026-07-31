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

import org.apache.flink.types.Row;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestHarnessTimerManagerTest {

    private static final Row P1 = Row.of("P1");
    private static final Row P2 = Row.of("P2");

    private static final ThrowingConsumer<Timer, Exception> NOOP_FIRER = t -> {};

    @Test
    void testNamedTimerRegistration() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, "timer-a");
        manager.register(P1, 2000L, "timer-b");

        assertThat(manager.getPendingTimers()).hasSize(2);
    }

    @Test
    void testNamedTimerReplacementOverwritesTimestamp() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();
        List<Timer> fired = new ArrayList<>();

        manager.register(P1, 1000L, "timer-a");
        manager.register(P1, 3000L, "timer-a");

        assertThat(manager.getPendingTimers()).hasSize(1);
        assertThat(manager.getPendingTimers().get(0).getTimestamp()).isEqualTo(3000L);

        manager.setTableWatermark("table-a", 2000L);
        manager.updateGlobalWatermarkAndFireTimers(fired::add);
        assertThat(fired).isEmpty();

        manager.setTableWatermark("table-a", 3000L);
        manager.updateGlobalWatermarkAndFireTimers(fired::add);
        assertThat(fired).hasSize(1);
        assertThat(fired.get(0).getTimestamp()).isEqualTo(3000L);
    }

    @Test
    void testNamedTimerReplacementIsPerPartition() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, "timer-a");
        manager.register(P2, 2000L, "timer-a");

        assertThat(manager.getPendingTimers()).hasSize(2);
    }

    @Test
    void testUnnamedTimerRegistration() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, null);
        manager.register(P1, 2000L, null);

        assertThat(manager.getPendingTimers()).hasSize(2);
    }

    @Test
    void testUnnamedTimerDeduplicationBySameTimestamp() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, null);
        manager.register(P1, 1000L, null);

        assertThat(manager.getPendingTimers()).hasSize(1);
    }

    @Test
    void testUnnamedTimerDeduplicationIsPerPartition() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, null);
        manager.register(P2, 1000L, null);

        assertThat(manager.getPendingTimers()).hasSize(2);
    }

    @Test
    void testNamedAndUnnamedTimersAreIndependent() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, "timer-a");
        manager.register(P1, 1000L, null);

        assertThat(manager.getPendingTimers()).hasSize(2);
    }

    @Test
    void testGlobalWatermarkIsMinAcrossTables() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.setTableWatermark("table-a", 1000L);
        manager.setTableWatermark("table-b", 3000L);
        manager.updateGlobalWatermarkAndFireTimers(NOOP_FIRER);

        assertThat(manager.getGlobalWatermark()).isEqualTo(1000L);
    }

    @Test
    void testGlobalWatermarkAdvancesWhenLaggingTableCatchesUp() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.setTableWatermark("table-a", 1000L);
        manager.setTableWatermark("table-b", 3000L);
        manager.updateGlobalWatermarkAndFireTimers(NOOP_FIRER);
        assertThat(manager.getGlobalWatermark()).isEqualTo(1000L);

        manager.setTableWatermark("table-a", 5000L);
        manager.updateGlobalWatermarkAndFireTimers(NOOP_FIRER);
        assertThat(manager.getGlobalWatermark()).isEqualTo(3000L);
    }

    @Test
    void testPerTableWatermarkCannotMoveBackward() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.setTableWatermark("table-a", 2000L);
        assertThrows(
                IllegalArgumentException.class, () -> manager.setTableWatermark("table-a", 1000L));
    }

    @Test
    void testPerTableWatermarkTrackedIndependently() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.setTableWatermark("table-a", 1000L);
        manager.setTableWatermark("table-b", 3000L);

        assertThat(manager.getWatermarkForTable("table-a")).isEqualTo(1000L);
        assertThat(manager.getWatermarkForTable("table-b")).isEqualTo(3000L);
    }

    @Test
    void testNewTableCanCauseGlobalWatermarkBackwardError() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.setTableWatermark("table-a", 5000L);
        manager.updateGlobalWatermarkAndFireTimers(NOOP_FIRER);
        assertThat(manager.getGlobalWatermark()).isEqualTo(5000L);

        manager.setTableWatermark("table-b", 1000L);
        assertThrows(
                IllegalArgumentException.class,
                () -> manager.updateGlobalWatermarkAndFireTimers(NOOP_FIRER));
    }

    @Test
    void testTimerFiresWhenWatermarkAdvancesPastTimestamp() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();
        List<Timer> fired = new ArrayList<>();

        manager.register(P1, 1000L, "timer-a");
        manager.setTableWatermark("table-a", 2000L);
        manager.updateGlobalWatermarkAndFireTimers(fired::add);

        assertThat(fired).hasSize(1);
        assertThat(fired.get(0).getName()).isEqualTo("timer-a");
        assertThat(manager.getPendingTimers()).isEmpty();
        assertThat(manager.getFiredTimers()).hasSize(1);
    }

    @Test
    void testTimerDoesNotFireBeforeWatermark() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();
        List<Timer> fired = new ArrayList<>();

        manager.register(P1, 3000L, "timer-a");
        manager.setTableWatermark("table-a", 2999L);
        manager.updateGlobalWatermarkAndFireTimers(fired::add);

        assertThat(fired).isEmpty();
        assertThat(manager.getPendingTimers()).hasSize(1);
    }

    @Test
    void testTimerFiresAtExactWatermark() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();
        List<Timer> fired = new ArrayList<>();

        manager.register(P1, 2000L, "timer-a");
        manager.setTableWatermark("table-a", 2000L);
        manager.updateGlobalWatermarkAndFireTimers(fired::add);

        assertThat(fired).hasSize(1);
    }

    @Test
    void testFiringOrderIsDeterministic() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();
        List<String> firingOrder = new ArrayList<>();

        manager.register(P1, 2000L, "b-timer");
        manager.register(P1, 1000L, "c-timer");
        manager.register(P1, 1000L, "a-timer");
        manager.register(P2, 1000L, "a-timer");

        manager.setTableWatermark("table-a", 3000L);
        manager.updateGlobalWatermarkAndFireTimers(
                t -> firingOrder.add(t.getKey() + ":" + t.getName()));

        assertThat(firingOrder)
                .containsExactly(
                        "+I[P1]:a-timer", "+I[P2]:a-timer", "+I[P1]:c-timer", "+I[P1]:b-timer");
    }

    @Test
    void testCascadingTimerFiring() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();
        List<String> fired = new ArrayList<>();

        manager.register(P1, 1000L, "first");
        manager.setTableWatermark("table-a", 3000L);
        manager.updateGlobalWatermarkAndFireTimers(
                t -> {
                    fired.add(t.getName());
                    if ("first".equals(t.getName())) {
                        manager.register(P1, 2000L, "within");
                        manager.register(P1, 5000L, "beyond");
                    }
                });

        assertThat(fired).containsExactly("first", "within");
        assertThat(manager.getPendingTimers()).hasSize(1);
        assertThat(manager.getPendingTimers().get(0).getName()).isEqualTo("beyond");
        assertThat(manager.getFiredTimers()).hasSize(2);
    }

    @Test
    void testTimerClearedDuringFiringCallbackDoesNotFire() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();
        List<String> fired = new ArrayList<>();

        manager.register(P1, 1000L, "first");
        manager.register(P1, 1000L, "second");

        assertThat(manager.getPendingTimers())
                .hasSize(2)
                .extracting(Timer::getName)
                .containsExactlyInAnyOrder("first", "second");

        manager.setTableWatermark("table-a", 2000L);
        manager.updateGlobalWatermarkAndFireTimers(
                t -> {
                    fired.add(t.getName());
                    if ("first".equals(t.getName())) {
                        manager.clearByName(P1, "second");
                    }
                });

        assertThat(fired).containsExactly("first");
        assertThat(manager.getPendingTimers()).isEmpty();
        assertThat(manager.getFiredTimers())
                .hasSize(1)
                .extracting(Timer::getName)
                .containsExactly("first");
    }

    @Test
    void testMultiTableTimerOnlyFiresWhenGlobalWatermarkAdvances() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();
        List<Timer> fired = new ArrayList<>();

        manager.register(P1, 2000L, "timer-a");
        manager.setTableWatermark("table-a", 1000L);
        manager.setTableWatermark("table-b", 500L);
        manager.updateGlobalWatermarkAndFireTimers(fired::add);
        assertThat(fired).isEmpty();

        manager.setTableWatermark("table-b", 3000L);
        manager.updateGlobalWatermarkAndFireTimers(fired::add);
        assertThat(fired).isEmpty();

        manager.setTableWatermark("table-a", 3000L);
        manager.updateGlobalWatermarkAndFireTimers(fired::add);
        assertThat(fired).hasSize(1);
    }

    @Test
    void testClearByName() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, "timer-a");
        manager.register(P1, 2000L, "timer-b");
        manager.clearByName(P1, "timer-a");

        assertThat(manager.getPendingTimers()).hasSize(1);
        assertThat(manager.getPendingTimers().get(0).getName()).isEqualTo("timer-b");
    }

    @Test
    void testClearByTimestampOnlyAffectsUnnamedTimers() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, "named");
        manager.register(P1, 1000L, null);
        manager.clearByTimestamp(P1, 1000L);

        assertThat(manager.getPendingTimers()).hasSize(1);
        assertThat(manager.getPendingTimers().get(0).getName()).isEqualTo("named");
    }

    @Test
    void testClearAll() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, "timer-a");
        manager.register(P1, 2000L, null);
        manager.clearAll(P1);

        assertThat(manager.getPendingTimers()).isEmpty();
    }

    @Test
    void testClearAllIsPerPartition() {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, "timer-a");
        manager.register(P2, 2000L, "timer-b");
        manager.clearAll(P1);

        assertThat(manager.getPendingTimers()).hasSize(1);
        assertThat(manager.getPendingTimers().get(0).getKey()).isEqualTo(P2);
    }

    @Test
    void testClearFiredTimers() throws Exception {
        TestHarnessTimerManager manager = new TestHarnessTimerManager();

        manager.register(P1, 1000L, "timer-a");
        manager.setTableWatermark("table-a", 2000L);
        manager.updateGlobalWatermarkAndFireTimers(NOOP_FIRER);

        assertThat(manager.getFiredTimers()).hasSize(1);
        manager.clearFiredTimers();
        assertThat(manager.getFiredTimers()).isEmpty();
    }
}
