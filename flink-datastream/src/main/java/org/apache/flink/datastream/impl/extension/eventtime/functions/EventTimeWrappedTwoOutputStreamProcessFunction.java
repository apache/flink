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

package org.apache.flink.datastream.impl.extension.eventtime.functions;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoOutputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.EventTimeExtensionImpl;
import org.apache.flink.datastream.impl.extension.eventtime.timer.DefaultEventTimeManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.function.Supplier;

/**
 * The wrapped {@link TwoOutputEventTimeStreamProcessFunction} that take care of event-time
 * alignment with idleness.
 */
public class EventTimeWrappedTwoOutputStreamProcessFunction<IN, OUT1, OUT2>
        implements TwoOutputStreamProcessFunction<IN, OUT1, OUT2> {

    private final TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2> wrappedUserFunction;

    private transient EventTimeManager eventTimeManager;

    private transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public EventTimeWrappedTwoOutputStreamProcessFunction(
            TwoOutputEventTimeStreamProcessFunction<IN, OUT1, OUT2> wrappedUserFunction) {
        this.wrappedUserFunction = Preconditions.checkNotNull(wrappedUserFunction);
    }

    @Override
    public void open(TwoOutputNonPartitionedContext<OUT1, OUT2> ctx) throws Exception {
        wrappedUserFunction.initEventTimeProcessFunction(eventTimeManager);
        wrappedUserFunction.open(ctx);
    }

    /**
     * Initialize the event time extension, note that this method should be invoked before open
     * method.
     */
    public void initEventTimeExtension(
            @Nullable InternalTimerService<VoidNamespace> timerService,
            Supplier<Long> eventTimeSupplier,
            EventTimeWatermarkHandler eventTimeWatermarkHandler) {
        this.eventTimeManager = new DefaultEventTimeManager(timerService, eventTimeSupplier);
        this.eventTimeWatermarkHandler = eventTimeWatermarkHandler;
    }

    @Override
    public void processRecord(
            IN record,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext<OUT1, OUT2> ctx)
            throws Exception {
        wrappedUserFunction.processRecord(record, output1, output2, ctx);
    }

    @Override
    public void endInput(TwoOutputNonPartitionedContext<OUT1, OUT2> ctx) throws Exception {
        wrappedUserFunction.endInput(ctx);
    }

    @Override
    public void onProcessingTimer(
            long timestamp,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext<OUT1, OUT2> ctx)
            throws Exception {
        wrappedUserFunction.onProcessingTimer(timestamp, output1, output2, ctx);
    }

    @Override
    public WatermarkHandlingResult onWatermark(
            Watermark watermark,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputNonPartitionedContext<OUT1, OUT2> ctx)
            throws Exception {
        if (EventTimeExtensionImpl.isEventTimeExtensionWatermark(watermark)) {
            // If the watermark is from the event time extension, process it and call {@code
            // userFunction#onEventTimeWatermark} when the event time is updated; otherwise, forward
            // the watermark.
            try {
                EventTimeWatermarkHandler.EventTimeUpdateStatus eventTimeUpdateStatus =
                        eventTimeWatermarkHandler.processWatermark(watermark, 0);
                if (eventTimeUpdateStatus.isEventTimeUpdated()) {
                    wrappedUserFunction.onEventTimeWatermark(
                            eventTimeUpdateStatus.getNewEventTime(), output1, output2, ctx);
                }
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
            // return POLL to indicate that the watermark has been processed
            return WatermarkHandlingResult.POLL;
        } else {
            return wrappedUserFunction.onWatermark(watermark, output1, output2, ctx);
        }
    }

    public void onEventTime(
            long timestamp,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext<OUT1, OUT2> ctx)
            throws Exception {
        wrappedUserFunction.onEventTimer(timestamp, output1, output2, ctx);
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return wrappedUserFunction.usesStates();
    }

    @Override
    public Set<? extends WatermarkDeclaration> declareWatermarks() {
        return wrappedUserFunction.declareWatermarks();
    }

    @Override
    public void close() throws Exception {
        wrappedUserFunction.close();
    }

    public TwoOutputStreamProcessFunction<IN, OUT1, OUT2> getWrappedUserFunction() {
        return wrappedUserFunction;
    }
}
