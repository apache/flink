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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;

/** Util methods for TimeService tests. */
public class TimeServiceTestUtils {

    public static <K, N>
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> createTimerQueue(
                    String name,
                    TimerSerializer<K, N> timerSerializer,
                    PriorityQueueSetFactory priorityQueueSetFactory) {
        return priorityQueueSetFactory.create(name, timerSerializer);
    }

    public static <K, N> InternalTimerServiceImpl<K, N> createInternalTimerService(
            KeyGroupRange keyGroupsList,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            PriorityQueueSetFactory priorityQueueSetFactory) {

        TimerSerializer<K, N> timerSerializer =
                new TimerSerializer<>(keySerializer, namespaceSerializer);

        return createInternalTimerService(
                keyGroupsList,
                keyContext,
                processingTimeService,
                createTimerQueue(
                        "__test_processing_timers", timerSerializer, priorityQueueSetFactory),
                createTimerQueue("__test_event_timers", timerSerializer, priorityQueueSetFactory));
    }

    public static <K, N> InternalTimerServiceImpl<K, N> createInternalTimerService(
            KeyGroupRange keyGroupsList,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue) {

        return new InternalTimerServiceImpl<>(
                keyGroupsList,
                keyContext,
                processingTimeService,
                processingTimeTimersQueue,
                eventTimeTimersQueue,
                StreamTaskCancellationContext.alwaysRunning());
    }

    public static <K, N> BacklogTimeService<K, N> createBacklogTimerService(
            ProcessingTimeService processingTimeService,
            Triggerable<K, N> triggerable,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue) {

        return new BacklogTimeService<>(processingTimeService, triggerable, eventTimeTimersQueue);
    }
}
