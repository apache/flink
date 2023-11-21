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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.streaming.api.operators.TimeServiceTestUtils.createBacklogTimerService;
import static org.apache.flink.streaming.api.operators.TimeServiceTestUtils.createTimerQueue;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BacklogTimeService}. */
public class BacklogTimeServiceTest {

    @Test
    public void testTriggerEventTimeTimer() throws Exception {
        List<Long> timers = new ArrayList<>();
        TestProcessingTimeService processingTimeService = new TestProcessingTimeService();
        KeyGroupRange testKeyGroupRange = new KeyGroupRange(0, 1);
        final HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(testKeyGroupRange, 1, 128);

        final TimerSerializer<Integer, String> timerSerializer =
                new TimerSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        final BacklogTimeService<Integer, String> timeService =
                createBacklogTimerService(
                        processingTimeService,
                        TestTrigger.eventTimeTrigger((timer) -> timers.add(timer.getTimestamp())),
                        createTimerQueue(
                                "eventTimerQueue", timerSerializer, priorityQueueSetFactory));

        timeService.setMaxWatermarkDuringBacklog(2);
        timeService.setCurrentKey(1);
        timeService.registerEventTimeTimer("a", 0);
        timeService.registerEventTimeTimer("a", 2);
        timeService.registerEventTimeTimer("a", 1);
        timeService.registerEventTimeTimer("a", 3);
        assertThat(timers).isEmpty();
        timeService.setCurrentKey(2);
        assertThat(timers).containsExactly(0L, 1L, 2L);
        timers.clear();

        timeService.registerEventTimeTimer("a", 2);
        timeService.registerEventTimeTimer("a", 1);
        timeService.registerEventTimeTimer("a", 3);
        assertThat(timers).isEmpty();
        timeService.setCurrentKey(null);
        assertThat(timers).containsExactly(1L, 2L);

        assertThat(timeService.currentWatermark()).isEqualTo(2);
    }
}
