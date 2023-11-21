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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.BiConsumerWithException;

/**
 * InternalBacklogAwareTimerServiceImpl is the implementation of InternalTimerService that manages
 * event time timers differently under different backlog status. It manages the processing time
 * timers in the same way as the {@link InternalTimerServiceImpl}.
 *
 * <p>The backlog status is false at the beginning. When backlog status is false, the timer services
 * is the same as the {@link InternalTimerServiceImpl}, where event time timers are triggered as the
 * watermark advancing.
 *
 * <p>When backlog status switch to true, the timer service manages event time timer with a single
 * active key at a time. When the active key changed, all the event time timers of the last key are
 * triggered up to the maximum watermark during backlog processing.
 *
 * <p>When the backlog status switch from true to false, all the event time timers of the last key
 * are triggered up to the maximum watermark during backlog processing. And the current watermark of
 * the timer service will be the maximum watermark during backlog processing.
 */
@Internal
public class InternalBacklogAwareTimerServiceImpl<K, N> implements InternalTimerService<N> {

    private final InternalTimerServiceImpl<K, N> realTimeInternalTimeService;
    private final BacklogTimeService<K, N> backlogTimeService;
    private InternalTimerService<N> currentInternalTimerService;

    public InternalBacklogAwareTimerServiceImpl(
            InternalTimerServiceImpl<K, N> realTimeInternalTimeService,
            BacklogTimeService<K, N> backlogTimeService) {
        this.realTimeInternalTimeService = realTimeInternalTimeService;
        this.backlogTimeService = backlogTimeService;
        this.currentInternalTimerService = realTimeInternalTimeService;
    }

    @Override
    public long currentProcessingTime() {
        return realTimeInternalTimeService.currentProcessingTime();
    }

    @Override
    public long currentWatermark() {
        return currentInternalTimerService.currentWatermark();
    }

    @Override
    public void registerProcessingTimeTimer(N namespace, long time) {
        realTimeInternalTimeService.registerProcessingTimeTimer(namespace, time);
    }

    @Override
    public void deleteProcessingTimeTimer(N namespace, long time) {
        realTimeInternalTimeService.deleteProcessingTimeTimer(namespace, time);
    }

    @Override
    public void registerEventTimeTimer(N namespace, long time) {
        currentInternalTimerService.registerEventTimeTimer(namespace, time);
    }

    @Override
    public void deleteEventTimeTimer(N namespace, long time) {
        currentInternalTimerService.deleteEventTimeTimer(namespace, time);
    }

    @Override
    public void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception {
        currentInternalTimerService.forEachEventTimeTimer(consumer);
    }

    @Override
    public void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer)
            throws Exception {
        realTimeInternalTimeService.forEachProcessingTimeTimer(consumer);
    }

    /**
     * Advances the Watermark of the {@link InternalTimerServiceImpl} during real time processing,
     * potentially firing event time timers.
     */
    public void advanceWatermark(long timestamp) throws Exception {
        realTimeInternalTimeService.advanceWatermark(timestamp);
    }

    /**
     * Set the maximum watermark during backlog of the {@link InternalBacklogAwareTimerServiceImpl}.
     */
    public void setMaxWatermarkDuringBacklog(long timestamp) {
        backlogTimeService.setMaxWatermarkDuringBacklog(timestamp);
    }

    /** Set the backlog status of the timer service. */
    public void setBacklog(boolean backlog) throws Exception {
        if (currentInternalTimerService == backlogTimeService && !backlog) {
            // Switch to non backlog
            backlogTimeService.setCurrentKey(null);
            currentInternalTimerService = realTimeInternalTimeService;
            realTimeInternalTimeService.advanceWatermark(backlogTimeService.currentWatermark());
            return;
        }

        if (currentInternalTimerService == realTimeInternalTimeService && backlog) {
            // Switch to backlog
            currentInternalTimerService = backlogTimeService;
        }
    }

    /**
     * Set the current key of the {@link InternalBacklogAwareTimerServiceImpl} during backlog
     * processing.
     */
    public void setCurrentKey(K newKey) throws Exception {
        if (currentInternalTimerService != backlogTimeService) {
            return;
        }
        backlogTimeService.setCurrentKey(newKey);
    }
}
