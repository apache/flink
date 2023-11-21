/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.streaming.api.operators.TimeServiceTestUtils.createBacklogTimerService;
import static org.apache.flink.streaming.api.operators.TimeServiceTestUtils.createInternalTimerService;
import static org.apache.flink.streaming.api.operators.TimeServiceTestUtils.createTimerQueue;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InternalBacklogAwareTimerServiceImpl}. */
public class InternalBacklogAwareTimerServiceImplTest {

    @Test
    public void testTriggerEventTimeTimerWithBacklog() throws Exception {
        KeyGroupRange testKeyGroupRange = new KeyGroupRange(0, 1);
        List<Tuple2<Integer, Long>> timers = new ArrayList<>();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        final HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(testKeyGroupRange, 1, 128);
        TimerSerializer<Integer, String> timerSerializer =
                new TimerSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        final TestTrigger<Integer, String> triggerable =
                TestTrigger.eventTimeTrigger(
                        (timer) -> timers.add(Tuple2.of(timer.getKey(), timer.getTimestamp())));
        final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, String>>
                eventTimersQueue =
                        createTimerQueue(
                                "eventTimersQueue", timerSerializer, priorityQueueSetFactory);
        final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, String>>
                processingTimerQueue =
                        createTimerQueue(
                                "processingTimerQueue", timerSerializer, priorityQueueSetFactory);
        final BacklogTimeService<Integer, String> backlogTimeService =
                createBacklogTimerService(processingTimeService, triggerable, eventTimersQueue);
        final TestKeyContext keyContext = new TestKeyContext();
        final InternalTimerServiceImpl<Integer, String> internalTimerService =
                createInternalTimerService(
                        testKeyGroupRange,
                        keyContext,
                        processingTimeService,
                        processingTimerQueue,
                        eventTimersQueue);
        internalTimerService.startTimerService(
                IntSerializer.INSTANCE, StringSerializer.INSTANCE, triggerable);

        final InternalBacklogAwareTimerServiceImpl<Integer, String> timerService =
                new InternalBacklogAwareTimerServiceImpl<>(
                        internalTimerService, backlogTimeService);

        keyContext.setCurrentKey(1);
        timerService.registerEventTimeTimer("a", 2);
        timerService.registerEventTimeTimer("a", 1);
        timerService.registerEventTimeTimer("a", 3);
        keyContext.setCurrentKey(2);
        timerService.registerEventTimeTimer("a", 3);
        timerService.registerEventTimeTimer("a", 1);
        timerService.advanceWatermark(2);
        assertThat(timers).containsExactly(Tuple2.of(1, 1L), Tuple2.of(2, 1L), Tuple2.of(1, 2L));
        timers.clear();

        // switch to backlog processing
        timerService.setMaxWatermarkDuringBacklog(5);
        timerService.setBacklog(true);
        timerService.setCurrentKey(1);
        timerService.registerEventTimeTimer("a", 5);
        timerService.registerEventTimeTimer("a", 4);
        timerService.setCurrentKey(2);
        timerService.registerEventTimeTimer("a", 6);
        timerService.setCurrentKey(null);
        assertThat(timers)
                .containsExactly(
                        Tuple2.of(1, 3L), Tuple2.of(1, 4L), Tuple2.of(1, 5L), Tuple2.of(2, 3L));
        timers.clear();

        // switch to non backlog processing
        timerService.setBacklog(false);
        assertThat(timerService.currentWatermark()).isEqualTo(5);
        keyContext.setCurrentKey(1);
        timerService.registerEventTimeTimer("a", 6);
        timerService.advanceWatermark(6);
        assertThat(timers).containsExactly(Tuple2.of(2, 6L), Tuple2.of(1, 6L));
    }
}
