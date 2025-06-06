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

package org.apache.flink.datastream.impl.extension.eventtime.functions;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoInputNonBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
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
 * The wrapped {@link TwoInputNonBroadcastEventTimeStreamProcessFunction} that take care of
 * event-time alignment with idleness.
 */
public class EventTimeWrappedTwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>
        implements TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> {

    private final TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT>
            wrappedUserFunction;

    private transient EventTimeManager eventTimeManager;

    private transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public EventTimeWrappedTwoInputNonBroadcastStreamProcessFunction(
            TwoInputNonBroadcastEventTimeStreamProcessFunction<IN1, IN2, OUT> wrappedUserFunction) {
        this.wrappedUserFunction = Preconditions.checkNotNull(wrappedUserFunction);
    }

    @Override
    public void open(NonPartitionedContext<OUT> ctx) throws Exception {
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
    public void processRecordFromFirstInput(
            IN1 record, Collector<OUT> output, PartitionedContext<OUT> ctx) throws Exception {
        wrappedUserFunction.processRecordFromFirstInput(record, output, ctx);
    }

    @Override
    public void processRecordFromSecondInput(
            IN2 record, Collector<OUT> output, PartitionedContext<OUT> ctx) throws Exception {
        wrappedUserFunction.processRecordFromSecondInput(record, output, ctx);
    }

    @Override
    public void endFirstInput(NonPartitionedContext<OUT> ctx) throws Exception {
        wrappedUserFunction.endFirstInput(ctx);
    }

    @Override
    public void endSecondInput(NonPartitionedContext<OUT> ctx) throws Exception {
        wrappedUserFunction.endSecondInput(ctx);
    }

    @Override
    public void onProcessingTimer(
            long timestamp, Collector<OUT> output, PartitionedContext<OUT> ctx) throws Exception {
        wrappedUserFunction.onProcessingTimer(timestamp, output, ctx);
    }

    @Override
    public WatermarkHandlingResult onWatermarkFromFirstInput(
            Watermark watermark, Collector<OUT> output, NonPartitionedContext<OUT> ctx)
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
                            eventTimeUpdateStatus.getNewEventTime(), output, ctx);
                }
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
            // return POLL to indicate that the watermark has been processed
            return WatermarkHandlingResult.POLL;
        } else {
            return wrappedUserFunction.onWatermarkFromFirstInput(watermark, output, ctx);
        }
    }

    @Override
    public WatermarkHandlingResult onWatermarkFromSecondInput(
            Watermark watermark, Collector<OUT> output, NonPartitionedContext<OUT> ctx)
            throws Exception {
        if (EventTimeExtensionImpl.isEventTimeExtensionWatermark(watermark)) {
            // If the watermark is from the event time extension, process it and call {@code
            // userFunction#onEventTimeWatermark} when the event time is updated; otherwise, forward
            // the watermark.
            try {
                EventTimeWatermarkHandler.EventTimeUpdateStatus eventTimeUpdateStatus =
                        eventTimeWatermarkHandler.processWatermark(watermark, 1);
                if (eventTimeUpdateStatus.isEventTimeUpdated()) {
                    wrappedUserFunction.onEventTimeWatermark(
                            eventTimeUpdateStatus.getNewEventTime(), output, ctx);
                }
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
            // return POLL to indicate that the watermark has been processed
            return WatermarkHandlingResult.POLL;
        } else {
            return wrappedUserFunction.onWatermarkFromSecondInput(watermark, output, ctx);
        }
    }

    public void onEventTime(long timestamp, Collector<OUT> output, PartitionedContext<OUT> ctx)
            throws Exception {
        wrappedUserFunction.onEventTimer(timestamp, output, ctx);
    }

    @Override
    public void close() throws Exception {
        wrappedUserFunction.close();
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return wrappedUserFunction.usesStates();
    }

    @Override
    public Set<? extends WatermarkDeclaration> declareWatermarks() {
        return wrappedUserFunction.declareWatermarks();
    }

    public TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> getWrappedUserFunction() {
        return wrappedUserFunction;
    }
}
