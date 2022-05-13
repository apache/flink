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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link BatchExecutionInternalTimeServiceManager} and {@link
 * BatchExecutionInternalTimeService}.
 */
public class BatchExecutionInternalTimeServiceTest extends TestLogger {
    public static final IntSerializer KEY_SERIALIZER = new IntSerializer();

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testBatchExecutionManagerCanBeInstantiatedWithBatchStateBackend() throws Exception {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(
                "Batch execution specific time service can work only with BatchExecutionKeyedStateBackend");

        MockEnvironment mockEnvironment = MockEnvironment.builder().build();
        AbstractKeyedStateBackend<Integer> stateBackend =
                new MemoryStateBackend()
                        .createKeyedStateBackend(
                                mockEnvironment,
                                new JobID(),
                                "dummy",
                                KEY_SERIALIZER,
                                2,
                                new KeyGroupRange(0, 1),
                                mockEnvironment.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                new CloseableRegistry());
        BatchExecutionInternalTimeServiceManager.create(
                stateBackend,
                this.getClass().getClassLoader(),
                new DummyKeyContext(),
                new TestProcessingTimeService(),
                Collections.emptyList());
    }

    @Test
    public void testForEachEventTimeTimerUnsupported() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "The BatchExecutionInternalTimeService should not be used in State Processor API");

        BatchExecutionInternalTimeService<Object, Object> timeService =
                new BatchExecutionInternalTimeService<>(
                        new TestProcessingTimeService(),
                        LambdaTrigger.eventTimeTrigger(timer -> {}));

        timeService.forEachEventTimeTimer(
                (o, aLong) -> fail("The forEachEventTimeTimer() should not be supported"));
    }

    @Test
    public void testForEachProcessingTimeTimerUnsupported() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "The BatchExecutionInternalTimeService should not be used in State Processor API");

        BatchExecutionInternalTimeService<Object, Object> timeService =
                new BatchExecutionInternalTimeService<>(
                        new TestProcessingTimeService(),
                        LambdaTrigger.eventTimeTrigger(timer -> {}));

        timeService.forEachEventTimeTimer(
                (o, aLong) -> fail("The forEachProcessingTimeTimer() should not be supported"));
    }

    @Test
    public void testFiringEventTimeTimers() throws Exception {
        BatchExecutionKeyedStateBackend<Integer> keyedStatedBackend =
                new BatchExecutionKeyedStateBackend<>(KEY_SERIALIZER, new KeyGroupRange(0, 1));
        InternalTimeServiceManager<Integer> timeServiceManager =
                BatchExecutionInternalTimeServiceManager.create(
                        keyedStatedBackend,
                        this.getClass().getClassLoader(),
                        new DummyKeyContext(),
                        new TestProcessingTimeService(),
                        Collections.emptyList());

        List<Long> timers = new ArrayList<>();
        InternalTimerService<VoidNamespace> timerService =
                timeServiceManager.getInternalTimerService(
                        "test",
                        KEY_SERIALIZER,
                        new VoidNamespaceSerializer(),
                        LambdaTrigger.eventTimeTrigger(timer -> timers.add(timer.getTimestamp())));

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 123);

        // advancing the watermark should not fire timers
        timeServiceManager.advanceWatermark(new Watermark(1000));
        timerService.deleteEventTimeTimer(VoidNamespace.INSTANCE, 123);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 150);

        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);

        assertThat(timers, equalTo(Collections.singletonList(150L)));
    }

    @Test
    public void testSettingSameKeyDoesNotFireTimers() {
        BatchExecutionKeyedStateBackend<Integer> keyedStatedBackend =
                new BatchExecutionKeyedStateBackend<>(KEY_SERIALIZER, new KeyGroupRange(0, 1));
        InternalTimeServiceManager<Integer> timeServiceManager =
                BatchExecutionInternalTimeServiceManager.create(
                        keyedStatedBackend,
                        this.getClass().getClassLoader(),
                        new DummyKeyContext(),
                        new TestProcessingTimeService(),
                        Collections.emptyList());

        List<Long> timers = new ArrayList<>();
        InternalTimerService<VoidNamespace> timerService =
                timeServiceManager.getInternalTimerService(
                        "test",
                        KEY_SERIALIZER,
                        new VoidNamespaceSerializer(),
                        LambdaTrigger.eventTimeTrigger(timer -> timers.add(timer.getTimestamp())));

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 123);
        keyedStatedBackend.setCurrentKey(1);

        assertThat(timers, equalTo(Collections.emptyList()));
    }

    @Test
    public void testCurrentWatermark() throws Exception {
        BatchExecutionKeyedStateBackend<Integer> keyedStatedBackend =
                new BatchExecutionKeyedStateBackend<>(KEY_SERIALIZER, new KeyGroupRange(0, 1));
        InternalTimeServiceManager<Integer> timeServiceManager =
                BatchExecutionInternalTimeServiceManager.create(
                        keyedStatedBackend,
                        this.getClass().getClassLoader(),
                        new DummyKeyContext(),
                        new TestProcessingTimeService(),
                        Collections.emptyList());

        List<Long> timers = new ArrayList<>();
        TriggerWithTimerServiceAccess<Integer, VoidNamespace> eventTimeTrigger =
                TriggerWithTimerServiceAccess.eventTimeTrigger(
                        (timer, timerService) -> {
                            assertThat(timerService.currentWatermark(), equalTo(Long.MAX_VALUE));
                            timers.add(timer.getTimestamp());
                        });
        InternalTimerService<VoidNamespace> timerService =
                timeServiceManager.getInternalTimerService(
                        "test", KEY_SERIALIZER, new VoidNamespaceSerializer(), eventTimeTrigger);
        eventTimeTrigger.setTimerService(timerService);

        assertThat(timerService.currentWatermark(), equalTo(Long.MIN_VALUE));
        keyedStatedBackend.setCurrentKey(1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 123);
        assertThat(timerService.currentWatermark(), equalTo(Long.MIN_VALUE));

        // advancing the watermark to a value different than Long.MAX_VALUE should have no effect
        timeServiceManager.advanceWatermark(new Watermark(1000));
        assertThat(timerService.currentWatermark(), equalTo(Long.MIN_VALUE));

        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);
        assertThat(timerService.currentWatermark(), equalTo(Long.MIN_VALUE));
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 124);

        // advancing the watermark to Long.MAX_VALUE should fire remaining key
        timeServiceManager.advanceWatermark(Watermark.MAX_WATERMARK);

        assertThat(timers, equalTo(Arrays.asList(123L, 124L)));
    }

    @Test
    public void testProcessingTimeTimers() {
        BatchExecutionKeyedStateBackend<Integer> keyedStatedBackend =
                new BatchExecutionKeyedStateBackend<>(KEY_SERIALIZER, new KeyGroupRange(0, 1));
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimeServiceManager<Integer> timeServiceManager =
                BatchExecutionInternalTimeServiceManager.create(
                        keyedStatedBackend,
                        this.getClass().getClassLoader(),
                        new DummyKeyContext(),
                        processingTimeService,
                        Collections.emptyList());

        List<Long> timers = new ArrayList<>();
        InternalTimerService<VoidNamespace> timerService =
                timeServiceManager.getInternalTimerService(
                        "test",
                        KEY_SERIALIZER,
                        new VoidNamespaceSerializer(),
                        LambdaTrigger.processingTimeTrigger(
                                timer -> timers.add(timer.getTimestamp())));

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 150);

        // we should never register physical timers
        assertThat(processingTimeService.getNumActiveTimers(), equalTo(0));
        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);

        assertThat(timers, equalTo(Collections.singletonList(150L)));
    }

    @Test
    public void testIgnoringEventTimeTimersFromWithinCallback() {
        BatchExecutionKeyedStateBackend<Integer> keyedStatedBackend =
                new BatchExecutionKeyedStateBackend<>(KEY_SERIALIZER, new KeyGroupRange(0, 1));
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimeServiceManager<Integer> timeServiceManager =
                BatchExecutionInternalTimeServiceManager.create(
                        keyedStatedBackend,
                        this.getClass().getClassLoader(),
                        new DummyKeyContext(),
                        processingTimeService,
                        Collections.emptyList());

        List<Long> timers = new ArrayList<>();
        TriggerWithTimerServiceAccess<Integer, VoidNamespace> trigger =
                TriggerWithTimerServiceAccess.eventTimeTrigger(
                        (timer, ts) -> {
                            timers.add(timer.getTimestamp());
                            ts.registerEventTimeTimer(
                                    VoidNamespace.INSTANCE, timer.getTimestamp() + 20);
                        });
        InternalTimerService<VoidNamespace> timerService =
                timeServiceManager.getInternalTimerService(
                        "test", KEY_SERIALIZER, new VoidNamespaceSerializer(), trigger);
        trigger.setTimerService(timerService);

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 150);

        // we should never register physical timers
        assertThat(processingTimeService.getNumActiveTimers(), equalTo(0));
        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);

        // We check that the timer from the callback is ignored
        assertThat(timers, equalTo(Collections.singletonList(150L)));
    }

    @Test
    public void testIgnoringProcessingTimeTimersFromWithinCallback() {
        BatchExecutionKeyedStateBackend<Integer> keyedStatedBackend =
                new BatchExecutionKeyedStateBackend<>(KEY_SERIALIZER, new KeyGroupRange(0, 1));
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        InternalTimeServiceManager<Integer> timeServiceManager =
                BatchExecutionInternalTimeServiceManager.create(
                        keyedStatedBackend,
                        this.getClass().getClassLoader(),
                        new DummyKeyContext(),
                        processingTimeService,
                        Collections.emptyList());

        List<Long> timers = new ArrayList<>();
        TriggerWithTimerServiceAccess<Integer, VoidNamespace> trigger =
                TriggerWithTimerServiceAccess.processingTimeTrigger(
                        (timer, ts) -> {
                            timers.add(timer.getTimestamp());
                            ts.registerProcessingTimeTimer(
                                    VoidNamespace.INSTANCE, timer.getTimestamp() + 20);
                        });
        InternalTimerService<VoidNamespace> timerService =
                timeServiceManager.getInternalTimerService(
                        "test", KEY_SERIALIZER, new VoidNamespaceSerializer(), trigger);
        trigger.setTimerService(timerService);

        keyedStatedBackend.setCurrentKey(1);
        timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 150);

        // we should never register physical timers
        assertThat(processingTimeService.getNumActiveTimers(), equalTo(0));
        // changing the current key fires all timers
        keyedStatedBackend.setCurrentKey(2);

        // We check that the timer from the callback is ignored
        assertThat(timers, equalTo(Collections.singletonList(150L)));
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
                            Assert.fail("We did not expect processing timer to be triggered."));
        }

        public static <K, N> TriggerWithTimerServiceAccess<K, N> processingTimeTrigger(
                BiConsumer<InternalTimer<K, N>, InternalTimerService<N>> processingTimeHandler) {
            return new TriggerWithTimerServiceAccess<>(
                    (timer, timeService) ->
                            Assert.fail("We did not expect event timer to be triggered."),
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
                    timer -> Assert.fail("We did not expect processing timer to be triggered."));
        }

        public static <K, N> LambdaTrigger<K, N> processingTimeTrigger(
                Consumer<InternalTimer<K, N>> processingTimeHandler) {
            return new LambdaTrigger<>(
                    timer -> Assert.fail("We did not expect event timer to be triggered."),
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
