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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.MockStateExecutor;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests for {@link BatchExecutionInternalTimeServiceManager} and {@link
 * BatchExecutionInternalTimeServiceWithAsyncState}.
 */
class BatchExecutionInternalTimeServiceWithAsyncStateTest {
    public static final IntSerializer KEY_SERIALIZER = new IntSerializer();

    BatchExecutionKeyedStateBackend<Integer> keyedStatedBackend;
    InternalTimeServiceManager<Integer> timeServiceManager;
    TestProcessingTimeService processingTimeService;
    AsyncExecutionController<Integer> aec;

    @BeforeEach
    public void setup() {
        keyedStatedBackend =
                new BatchExecutionKeyedStateBackend<>(
                        KEY_SERIALIZER, new KeyGroupRange(0, 1), new ExecutionConfig());
        processingTimeService = new TestProcessingTimeService();
        aec =
                new AsyncExecutionController<>(
                        new SyncMailboxExecutor(),
                        (a, b) -> {},
                        new MockStateExecutor(),
                        new DeclarationManager(),
                        1,
                        100,
                        1000,
                        1,
                        null,
                        null);
        timeServiceManager =
                BatchExecutionInternalTimeServiceManager.create(
                        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup()
                                .getIOMetricGroup(),
                        new AsyncKeyedStateBackendAdaptor<>(keyedStatedBackend),
                        null,
                        this.getClass().getClassLoader(),
                        new DummyKeyContext(),
                        processingTimeService,
                        Collections.emptyList(),
                        StreamTaskCancellationContext.alwaysRunning());
    }

    @Test
    void testForEachEventTimeTimerUnsupported() {
        BatchExecutionInternalTimeServiceWithAsyncState<Object, Object> timeService =
                new BatchExecutionInternalTimeServiceWithAsyncState<>(
                        new TestProcessingTimeService(),
                        LambdaTrigger.eventTimeTrigger(timer -> {}));

        assertThatThrownBy(
                        () ->
                                timeService.forEachEventTimeTimer(
                                        (o, aLong) ->
                                                fail(
                                                        "The forEachEventTimeTimer() should not be supported")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "The BatchExecutionInternalTimeService should not be used in State Processor API");
    }

    @Test
    void testForEachProcessingTimeTimerUnsupported() {
        BatchExecutionInternalTimeServiceWithAsyncState<Object, Object> timeService =
                new BatchExecutionInternalTimeServiceWithAsyncState<>(
                        new TestProcessingTimeService(),
                        LambdaTrigger.eventTimeTrigger(timer -> {}));

        assertThatThrownBy(
                        () ->
                                timeService.forEachEventTimeTimer(
                                        (o, aLong) ->
                                                fail(
                                                        "The forEachProcessingTimeTimer() should not be supported")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "The BatchExecutionInternalTimeService should not be used in State Processor API");
    }

    @Test
    void testFiringEventTimeTimers() throws Exception {
        List<Long> timers = new ArrayList<>();
        InternalTimerService<VoidNamespace> timerService =
                buildTimerService(
                        LambdaTrigger.eventTimeTrigger(timer -> timers.add(timer.getTimestamp())));

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 123);

        // advancing the watermark should not fire timers
        timeServiceManager.advanceWatermark(new Watermark(1000));
        timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, 123);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 150);

        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);

        assertThat(timers).containsExactly(150L);
    }

    @Test
    void testSettingSameKeyDoesNotFireTimers() {
        List<Long> timers = new ArrayList<>();
        InternalTimerService<VoidNamespace> timerService =
                buildTimerService(
                        LambdaTrigger.eventTimeTrigger(timer -> timers.add(timer.getTimestamp())));

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 123);
        keyedStatedBackend.setCurrentKey(1);

        assertThat(timers).isEmpty();
    }

    @Test
    void testCurrentWatermark() throws Exception {
        List<Long> timers = new ArrayList<>();
        TriggerWithTimerServiceAccess<Integer, VoidNamespace> eventTimeTrigger =
                TriggerWithTimerServiceAccess.eventTimeTrigger(
                        (timer, timerService) -> {
                            assertThat(timerService.currentWatermark()).isEqualTo(Long.MAX_VALUE);
                            timers.add(timer.getTimestamp());
                        });
        InternalTimerService<VoidNamespace> timerService = buildTimerService(eventTimeTrigger);
        eventTimeTrigger.setTimerService(timerService);

        assertThat(timerService.currentWatermark()).isEqualTo(Long.MIN_VALUE);
        keyedStatedBackend.setCurrentKey(1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 123);
        assertThat(timerService.currentWatermark()).isEqualTo(Long.MIN_VALUE);

        // advancing the watermark to a value different than Long.MAX_VALUE should have no effect
        timeServiceManager.advanceWatermark(new Watermark(1000));
        assertThat(timerService.currentWatermark()).isEqualTo(Long.MIN_VALUE);

        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);
        assertThat(timerService.currentWatermark()).isEqualTo(Long.MIN_VALUE);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 124);

        // advancing the watermark to Long.MAX_VALUE should fire remaining key
        timeServiceManager.advanceWatermark(Watermark.MAX_WATERMARK);

        assertThat(timers).containsExactly(123L, 124L);
    }

    @Test
    void testProcessingTimeTimers() {
        List<Long> timers = new ArrayList<>();
        InternalTimerService<VoidNamespace> timerService =
                buildTimerService(
                        LambdaTrigger.processingTimeTrigger(
                                timer -> timers.add(timer.getTimestamp())));

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 150);

        // we should never register physical timers
        assertThat(processingTimeService.getNumActiveTimers()).isZero();
        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);

        assertThat(timers).containsExactly(150L);
    }

    @Test
    void testIgnoringEventTimeTimersFromWithinCallback() {
        List<Long> timers = new ArrayList<>();
        TriggerWithTimerServiceAccess<Integer, VoidNamespace> trigger =
                TriggerWithTimerServiceAccess.eventTimeTrigger(
                        (timer, ts) -> {
                            timers.add(timer.getTimestamp());
                            ts.registerEventTimeTimer(
                                    VoidNamespace.INSTANCE, timer.getTimestamp() + 20);
                        });
        InternalTimerService<VoidNamespace> timerService = buildTimerService(trigger);
        trigger.setTimerService(timerService);

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 150);

        // we should never register physical timers
        assertThat(processingTimeService.getNumActiveTimers()).isZero();
        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);

        // We check that the timer from the callback is ignored
        assertThat(timers).containsExactly(150L);
    }

    @Test
    void testIgnoringProcessingTimeTimersFromWithinCallback() {
        List<Long> timers = new ArrayList<>();
        TriggerWithTimerServiceAccess<Integer, VoidNamespace> trigger =
                TriggerWithTimerServiceAccess.processingTimeTrigger(
                        (timer, ts) -> {
                            timers.add(timer.getTimestamp());
                            ts.registerProcessingTimeTimer(
                                    VoidNamespace.INSTANCE, timer.getTimestamp() + 20);
                        });
        InternalTimerService<VoidNamespace> timerService = buildTimerService(trigger);
        trigger.setTimerService(timerService);

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 150);

        // we should never register physical timers
        assertThat(processingTimeService.getNumActiveTimers()).isZero();
        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);

        // We check that the timer from the callback is ignored
        assertThat(timers).containsExactly(150L);
    }

    private InternalTimerService<VoidNamespace> buildTimerService(
            Triggerable<Integer, VoidNamespace> trigger) {
        InternalTimerService<VoidNamespace> timerService =
                timeServiceManager.getInternalTimerService(
                        "test", KEY_SERIALIZER, new VoidNamespaceSerializer(), trigger);
        ((BatchExecutionInternalTimeServiceWithAsyncState<Integer, VoidNamespace>) timerService)
                .setup(aec);
        return timerService;
    }

    private static class TriggerWithTimerServiceAccess<K, N> implements Triggerable<K, N> {

        private InternalTimerService<N> timerService;
        private final BiConsumer<InternalTimer<K, N>, InternalTimerService<N>> eventTimeHandler;
        private final BiConsumer<InternalTimer<K, N>, InternalTimerService<N>>
                processingTimeHandler;

        private TriggerWithTimerServiceAccess(
                BiConsumer<InternalTimer<K, N>, InternalTimerService<N>> eventTimeHandler,
                BiConsumer<InternalTimer<K, N>, InternalTimerService<N>> processingTimeHandler) {
            this.eventTimeHandler = eventTimeHandler;
            this.processingTimeHandler = processingTimeHandler;
        }

        public static <K, N> TriggerWithTimerServiceAccess<K, N> eventTimeTrigger(
                BiConsumer<InternalTimer<K, N>, InternalTimerService<N>> eventTimeHandler) {
            return new TriggerWithTimerServiceAccess<>(
                    eventTimeHandler,
                    (timer, timeService) ->
                            fail("We did not expect processing timer to be triggered."));
        }

        public static <K, N> TriggerWithTimerServiceAccess<K, N> processingTimeTrigger(
                BiConsumer<InternalTimer<K, N>, InternalTimerService<N>> processingTimeHandler) {
            return new TriggerWithTimerServiceAccess<>(
                    (timer, timeService) -> fail("We did not expect event timer to be triggered."),
                    processingTimeHandler);
        }

        public void setTimerService(InternalTimerService<N> timerService) {
            this.timerService = timerService;
        }

        @Override
        public void onEventTime(InternalTimer<K, N> timer) throws Exception {
            this.eventTimeHandler.accept(timer, timerService);
        }

        @Override
        public void onProcessingTime(InternalTimer<K, N> timer) throws Exception {
            this.processingTimeHandler.accept(timer, timerService);
        }
    }

    private static class LambdaTrigger<K, N> implements Triggerable<K, N> {

        private final Consumer<InternalTimer<K, N>> eventTimeHandler;
        private final Consumer<InternalTimer<K, N>> processingTimeHandler;

        public static <K, N> LambdaTrigger<K, N> eventTimeTrigger(
                Consumer<InternalTimer<K, N>> eventTimeHandler) {
            return new LambdaTrigger<>(
                    eventTimeHandler,
                    timer -> fail("We did not expect processing timer to be triggered."));
        }

        public static <K, N> LambdaTrigger<K, N> processingTimeTrigger(
                Consumer<InternalTimer<K, N>> processingTimeHandler) {
            return new LambdaTrigger<>(
                    timer -> fail("We did not expect event timer to be triggered."),
                    processingTimeHandler);
        }

        private LambdaTrigger(
                Consumer<InternalTimer<K, N>> eventTimeHandler,
                Consumer<InternalTimer<K, N>> processingTimeHandler) {
            this.eventTimeHandler = eventTimeHandler;
            this.processingTimeHandler = processingTimeHandler;
        }

        @Override
        public void onEventTime(InternalTimer<K, N> timer) throws Exception {
            this.eventTimeHandler.accept(timer);
        }

        @Override
        public void onProcessingTime(InternalTimer<K, N> timer) throws Exception {
            this.processingTimeHandler.accept(timer);
        }
    }

    private static class DummyKeyContext implements KeyContext {
        @Override
        public void setCurrentKey(Object key) {}

        @Override
        public Object getCurrentKey() {
            return null;
        }
    }
}
