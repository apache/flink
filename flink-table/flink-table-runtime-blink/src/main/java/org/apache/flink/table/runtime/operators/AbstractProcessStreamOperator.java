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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * AbstractProcessStreamOperator is a base class for stream operators without key.
 *
 * @param <OUT> The output type of the operator.
 */
public abstract class AbstractProcessStreamOperator<OUT> extends TableStreamOperator<OUT> {

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    protected long currentWatermark = Long.MIN_VALUE;

    protected transient ContextImpl ctx;

    @Override
    public void open() throws Exception {
        super.open();
        this.ctx = new ContextImpl(getProcessingTimeService());
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        currentWatermark = mark.getTimestamp();
    }

    /** Information available in an invocation of processElement. */
    protected class ContextImpl implements TimerService {

        protected final ProcessingTimeService timerService;

        public StreamRecord<?> element;

        ContextImpl(ProcessingTimeService timerService) {
            this.timerService = checkNotNull(timerService);
        }

        public Long timestamp() {
            checkState(element != null);

            if (element.hasTimestamp()) {
                return element.getTimestamp();
            } else {
                return null;
            }
        }

        @Override
        public long currentProcessingTime() {
            return timerService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(
                    "Setting timers is only supported on a keyed streams.");
        }

        @Override
        public void registerEventTimeTimer(long time) {
            throw new UnsupportedOperationException(
                    "Setting timers is only supported on a keyed streams.");
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException(
                    "Delete timers is only supported on a keyed streams.");
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            throw new UnsupportedOperationException(
                    "Delete timers is only supported on a keyed streams.");
        }

        public TimerService timerService() {
            return this;
        }
    }
}
