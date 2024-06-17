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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.state.StateFutureImpl.AsyncFrameworkExceptionHandler;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.MockStateExecutor;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InternalTimerServiceAsyncImpl}. */
class InternalTimerServiceAsyncImplTest {
    private AsyncExecutionController asyncExecutionController;
    private TestKeyContext keyContext;
    private TestProcessingTimeService processingTimeService;
    private InternalTimerServiceAsyncImpl<Integer, String> service;

    private AsyncFrameworkExceptionHandler exceptionHandler =
            new AsyncFrameworkExceptionHandler() {
                @Override
                public void handleException(String message, Throwable exception) {
                    throw new RuntimeException(message, exception);
                }
            };

    @BeforeEach
    void setup() throws Exception {
        asyncExecutionController =
                new AsyncExecutionController(
                        new SyncMailboxExecutor(),
                        exceptionHandler,
                        new MockStateExecutor(),
                        128,
                        2,
                        1000L,
                        10);
        // ensure arbitrary key is in the key group
        int totalKeyGroups = 128;
        KeyGroupRange testKeyGroupList = new KeyGroupRange(0, totalKeyGroups - 1);

        keyContext = new TestKeyContext();

        processingTimeService = new TestProcessingTimeService();
        processingTimeService.setCurrentTime(0L);
        PriorityQueueSetFactory factory =
                new HeapPriorityQueueSetFactory(testKeyGroupList, totalKeyGroups, 128);
        service =
                createInternalTimerService(
                        UnregisteredMetricGroups.createUnregisteredTaskMetricGroup()
                                .getIOMetricGroup(),
                        testKeyGroupList,
                        keyContext,
                        processingTimeService,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        factory,
                        asyncExecutionController);
        TestTriggerable.processingTriggerCount = 0;
        TestTriggerable.eventTriggerCount = 0;
    }

    @Test
    void testTimerWithSameKey() throws Exception {
        keyContext.setCurrentKey("key-1");
        service.registerProcessingTimeTimer("processing-timer-1", 1L);
        service.registerProcessingTimeTimer("processing-timer-2", 1L);
        TestTriggerable testTriggerable = new TestTriggerable();
        service.startTimerService(
                IntSerializer.INSTANCE, StringSerializer.INSTANCE, testTriggerable);
        assertThat(testTriggerable.processingTriggerCount).isEqualTo(0);
        processingTimeService.advance(1);
        assertThat(testTriggerable.processingTriggerCount).isEqualTo(2);
    }

    @Test
    void testProcessingTimerFireOrder() throws Exception {
        keyContext.setCurrentKey("key-1");
        service.registerProcessingTimeTimer("processing-timer-1", 1L);

        TestTriggerable testTriggerable = new TestTriggerable();
        service.startTimerService(
                IntSerializer.INSTANCE, StringSerializer.INSTANCE, testTriggerable);
        assertThat(testTriggerable.processingTriggerCount).isEqualTo(0);
        // the processing timer should be triggered at time 1
        processingTimeService.advance(1);
        assertThat(testTriggerable.processingTriggerCount).isEqualTo(1);

        keyContext.setCurrentKey("key-2");
        service.registerProcessingTimeTimer("processing-timer-2", 2L);
        assertThat(testTriggerable.processingTriggerCount).isEqualTo(1);

        RecordContext<String> recordContext =
                asyncExecutionController.buildContext("record2", "key-2");
        asyncExecutionController.setCurrentContext(recordContext);
        asyncExecutionController.handleRequest(null, StateRequestType.VALUE_GET, null);
        processingTimeService.advance(1);
        // timer fire is blocked by record2's value_get
        assertThat(testTriggerable.processingTriggerCount).isEqualTo(1);
        // record2 finished, key-2 is released, timer fire can be triggered
        recordContext.release();
        processingTimeService.advance(1);
        assertThat(testTriggerable.processingTriggerCount).isEqualTo(2);
    }

    @Test
    void testEventTimerFireOrder() throws Exception {
        keyContext.setCurrentKey("key-1");
        service.registerEventTimeTimer("event-timer-1", 1L);

        TestTriggerable testTriggerable = new TestTriggerable();
        service.startTimerService(
                IntSerializer.INSTANCE, StringSerializer.INSTANCE, testTriggerable);
        assertThat(testTriggerable.eventTriggerCount).isEqualTo(0);
        // the event timer should be triggered at watermark 1
        service.advanceWatermark(1L);
        assertThat(testTriggerable.eventTriggerCount).isEqualTo(1);

        keyContext.setCurrentKey("key-2");
        service.registerEventTimeTimer("event-timer-2", 2L);
        assertThat(testTriggerable.eventTriggerCount).isEqualTo(1);

        RecordContext<String> recordContext =
                asyncExecutionController.buildContext("record2", "key-2");
        asyncExecutionController.setCurrentContext(recordContext);
        asyncExecutionController.handleRequest(null, StateRequestType.VALUE_GET, null);
        service.advanceWatermark(2L);
        // timer fire is blocked by record2's value_get
        assertThat(testTriggerable.eventTriggerCount).isEqualTo(1);
        // record2 finished, key-2 is released, timer fire can be triggered
        recordContext.release();
        service.advanceWatermark(3L);
        assertThat(testTriggerable.eventTriggerCount).isEqualTo(2);
    }

    private static <K, N> InternalTimerServiceAsyncImpl<K, N> createInternalTimerService(
            TaskIOMetricGroup taskIOMetricGroup,
            KeyGroupRange keyGroupsList,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            PriorityQueueSetFactory priorityQueueSetFactory,
            AsyncExecutionController asyncExecutionController) {

        TimerSerializer<K, N> timerSerializer =
                new TimerSerializer<>(keySerializer, namespaceSerializer);

        return new InternalTimerServiceAsyncImpl<>(
                taskIOMetricGroup,
                keyGroupsList,
                keyContext,
                processingTimeService,
                priorityQueueSetFactory.create("__async_processing_timers", timerSerializer),
                priorityQueueSetFactory.create("__async_event_timers", timerSerializer),
                StreamTaskCancellationContext.alwaysRunning(),
                asyncExecutionController);
    }

    private static class TestTriggerable implements Triggerable<Integer, String> {

        private static int eventTriggerCount = 0;

        private static int processingTriggerCount = 0;

        @Override
        public void onEventTime(InternalTimer<Integer, String> timer) throws Exception {
            eventTriggerCount++;
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, String> timer) throws Exception {
            processingTriggerCount++;
        }
    }

    private static class TestKeyContext implements KeyContext {

        private Object key;

        @Override
        public void setCurrentKey(Object key) {
            this.key = key;
        }

        @Override
        public Object getCurrentKey() {
            return key;
        }
    }
}
