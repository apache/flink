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

import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionInternalTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.LinkedList;
import java.util.List;

/**
 * An implementation of a {@link InternalTimerService} that manages timers with a single active key
 * at a time. This is used by {@link
 * org.apache.flink.streaming.api.operators.InternalBacklogAwareTimerServiceImpl} during backlog
 * processing.
 */
class BacklogTimeService<K, N> extends BatchExecutionInternalTimeService<K, N> {
    private long maxWatermarkDuringBacklog;

    public BacklogTimeService(
            ProcessingTimeService processingTimeService,
            Triggerable<K, N> triggerTarget,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue) {
        super(processingTimeService, triggerTarget, eventTimeTimersQueue, null);
    }

    @Override
    public void registerProcessingTimeTimer(N namespace, long time) {
        throw new UnsupportedOperationException(
                "BacklogTimeService does not support registering processing timer.");
    }

    @Override
    public void deleteProcessingTimeTimer(N namespace, long time) {
        throw new UnsupportedOperationException(
                "BacklogTimeService does not support deleting processing timer.");
    }

    /**
     * Set the current key of the time service. If the new key is different from the last key, all
     * the event time timers of the last key whose timestamp is less than or equal to the max
     * watermark during backlog are triggered.
     */
    public void setCurrentKey(K newKey) throws Exception {
        if (newKey != null && newKey.equals(currentKey)) {
            return;
        }

        TimerHeapInternalTimer<K, N> timer;
        List<TimerHeapInternalTimer<K, N>> skippedTimers = new LinkedList<>();
        if (currentKey != null) {
            while ((timer = eventTimeTimersQueue.peek()) != null
                    && timer.getTimestamp() <= maxWatermarkDuringBacklog) {
                eventTimeTimersQueue.poll();

                if (timer.getKey() != currentKey) {
                    skippedTimers.add(timer);
                } else {
                    triggerTarget.onEventTime(timer);
                }
            }
            eventTimeTimersQueue.addAll(skippedTimers);
        }

        if (newKey == null) {
            currentWatermark = maxWatermarkDuringBacklog;
        }

        currentKey = newKey;
    }

    public void setMaxWatermarkDuringBacklog(long watermark) {
        maxWatermarkDuringBacklog = watermark;
    }
}
