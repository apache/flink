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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * An implementation of {@link InternalTimerService} that is used by {@link
 * org.apache.flink.streaming.runtime.operators.asyncprocessing.AbstractAsyncStateStreamOperator}.
 * The timer service will set {@link RecordContext} for the timers before invoking action to
 * preserve the execution order between timer firing and records processing.
 *
 * @see <a
 *     href=https://cwiki.apache.org/confluence/display/FLINK/FLIP-425%3A+Asynchronous+Execution+Model#FLIP425:AsynchronousExecutionModel-Timers>FLIP-425
 *     timers section.</a>
 * @param <K> Type of timer's key.
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public class InternalTimerServiceAsyncImpl<K, N> extends InternalTimerServiceImpl<K, N> {

    private AsyncExecutionController<K> asyncExecutionController;

    InternalTimerServiceAsyncImpl(
            TaskIOMetricGroup taskIOMetricGroup,
            KeyGroupRange localKeyGroupRange,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue,
            StreamTaskCancellationContext cancellationContext,
            AsyncExecutionController<K> asyncExecutionController) {
        super(
                taskIOMetricGroup,
                localKeyGroupRange,
                keyContext,
                processingTimeService,
                processingTimeTimersQueue,
                eventTimeTimersQueue,
                cancellationContext);
        this.asyncExecutionController = asyncExecutionController;
    }

    @Override
    void onProcessingTime(long time) throws Exception {
        // null out the timer in case the Triggerable calls registerProcessingTimeTimer()
        // inside the callback.
        nextTimer = null;

        InternalTimer<K, N> timer;

        while ((timer = processingTimeTimersQueue.peek()) != null
                && timer.getTimestamp() <= time
                && !cancellationContext.isCancelled()) {
            processingTimeTimersQueue.poll();
            final InternalTimer<K, N> timerToTrigger = timer;
            maintainContextAndProcess(
                    timerToTrigger, () -> triggerTarget.onProcessingTime(timerToTrigger));
            taskIOMetricGroup.getNumFiredTimers().inc();
        }

        if (timer != null && nextTimer == null) {
            nextTimer =
                    processingTimeService.registerTimer(
                            timer.getTimestamp(), this::onProcessingTime);
        }
    }

    /**
     * Advance one watermark, this will fire some event timers.
     *
     * @param time the time in watermark.
     */
    @Override
    public void advanceWatermark(long time) throws Exception {
        currentWatermark = time;

        InternalTimer<K, N> timer;

        while ((timer = eventTimeTimersQueue.peek()) != null
                && timer.getTimestamp() <= time
                && !cancellationContext.isCancelled()) {
            eventTimeTimersQueue.poll();
            final InternalTimer<K, N> timerToTrigger = timer;
            maintainContextAndProcess(
                    timerToTrigger, () -> triggerTarget.onEventTime(timerToTrigger));
            taskIOMetricGroup.getNumFiredTimers().inc();
        }
    }

    /**
     * Iterator each timer in the queue, and invoke the consumer. This function is mainly used by
     * state-processor-API. TODO: Ensure state-processor-API that only uses sync state API.
     */
    protected void foreachTimer(
            BiConsumerWithException<N, Long, Exception> consumer,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> queue)
            throws Exception {
        throw new UnsupportedOperationException(
                "Batch operation is not supported when using async state.");
    }

    private void maintainContextAndProcess(
            InternalTimer<K, N> timer, ThrowingRunnable<Exception> runnable) {
        RecordContext<K> recordCtx = asyncExecutionController.buildContext(null, timer.getKey());
        recordCtx.retain();
        asyncExecutionController.setCurrentContext(recordCtx);
        keyContext.setCurrentKey(timer.getKey());
        asyncExecutionController.syncPointRequestWithCallback(runnable);
        recordCtx.release();
    }
}
