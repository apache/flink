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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * An implementation of a {@link InternalTimerService} that manages timers with a single active key
 * at a time. Can be used in a BATCH execution mode cooperating with async state operators.
 */
@Internal
public class BatchExecutionInternalTimeServiceWithAsyncState<K, N>
        extends BatchExecutionInternalTimeService<K, N> {

    private AsyncExecutionController<K, ?> asyncExecutionController;

    BatchExecutionInternalTimeServiceWithAsyncState(
            ProcessingTimeService processingTimeService, Triggerable<K, N> triggerTarget) {
        super(processingTimeService, triggerTarget);
    }

    /** Set up the async execution controller. */
    public void setup(AsyncExecutionController<K, ?> asyncExecutionController) {
        if (asyncExecutionController != null) {
            this.asyncExecutionController = asyncExecutionController;
        }
    }

    /**
     * Sets the current key. Timers that are due to be fired are collected and will be triggered.
     */
    @Override
    public void setCurrentKey(K currentKey) throws Exception {
        if (currentKey != null && currentKey.equals(this.currentKey)) {
            return;
        }
        currentWatermark = Long.MAX_VALUE;
        InternalTimer<K, N> timer;
        while ((timer = eventTimeTimersQueue.poll()) != null) {
            final InternalTimer<K, N> timerToTrigger = timer;
            maintainContextAndProcess(
                    timerToTrigger, () -> triggerTarget.onEventTime(timerToTrigger));
        }
        while ((timer = processingTimeTimersQueue.poll()) != null) {
            final InternalTimer<K, N> timerToTrigger = timer;
            maintainContextAndProcess(
                    timerToTrigger, () -> triggerTarget.onProcessingTime(timerToTrigger));
        }
        currentWatermark = Long.MIN_VALUE;
        this.currentKey = currentKey;
    }

    private void maintainContextAndProcess(
            InternalTimer<K, N> timer, ThrowingRunnable<Exception> runnable) {
        // Since we are in middle of processing a record, we need to maintain the context.
        final RecordContext<K> previousContext = asyncExecutionController.getCurrentContext();
        RecordContext<K> recordCtx = asyncExecutionController.buildContext(timer, timer.getKey());
        recordCtx.retain();
        asyncExecutionController.setCurrentContext(recordCtx);
        asyncExecutionController.syncPointRequestWithCallback(runnable, true);
        recordCtx.release();
        asyncExecutionController.setCurrentContext(previousContext);
    }
}
