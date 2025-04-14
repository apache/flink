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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.datastream.impl.extension.eventtime.EventTimeExtensionImpl;
import org.apache.flink.datastream.impl.extension.eventtime.functions.EventTimeWrappedTwoInputBroadcastStreamProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateUdfStreamOperator;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermark.AbstractInternalWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Operator for {@link TwoInputBroadcastStreamProcessFunction}. */
public class TwoInputBroadcastProcessOperator<IN1, IN2, OUT>
        extends AbstractAsyncStateUdfStreamOperator<
                OUT, TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, BoundedMultiInput {

    protected transient TimestampCollector<OUT> collector;

    protected transient DefaultRuntimeContext context;

    protected transient DefaultPartitionedContext<OUT> partitionedContext;

    protected transient NonPartitionedContext<OUT> nonPartitionedContext;

    protected transient Map<String, AbstractInternalWatermarkDeclaration<?>>
            watermarkDeclarationMap;

    // {@link EventTimeWatermarkHandler} will be used to process event time related watermarks
    protected transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public TwoInputBroadcastProcessOperator(
            TwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT> userFunction) {
        super(userFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = getOutputCollector();
        StreamingRuntimeContext operatorContext = getRuntimeContext();
        TaskInfo taskInfo = operatorContext.getTaskInfo();
        this.context =
                new DefaultRuntimeContext(
                        operatorContext.getJobInfo().getJobName(),
                        operatorContext.getJobType(),
                        taskInfo.getNumberOfParallelSubtasks(),
                        taskInfo.getMaxNumberOfParallelSubtasks(),
                        taskInfo.getTaskName(),
                        taskInfo.getIndexOfThisSubtask(),
                        taskInfo.getAttemptNumber(),
                        operatorContext.getMetricGroup());

        watermarkDeclarationMap =
                config.getWatermarkDeclarations(getUserCodeClassloader()).stream()
                        .collect(
                                Collectors.toMap(
                                        AbstractInternalWatermarkDeclaration::getIdentifier,
                                        Function.identity()));
        this.partitionedContext =
                new DefaultPartitionedContext<>(
                        context,
                        this::currentKey,
                        getProcessorWithKey(),
                        getProcessingTimeManager(),
                        operatorContext,
                        getOperatorStateBackend());
        this.nonPartitionedContext = getNonPartitionedContext();
        this.partitionedContext.setNonPartitionedContext(this.nonPartitionedContext);
        this.eventTimeWatermarkHandler =
                new EventTimeWatermarkHandler(2, output, timeServiceManager);

        if (userFunction instanceof EventTimeWrappedTwoInputBroadcastStreamProcessFunction) {
            // note that the {@code initEventTimeExtension} in EventTimeWrappedProcessFunction
            // should be invoked before the {@code open}.
            ((EventTimeWrappedTwoInputBroadcastStreamProcessFunction<IN1, IN2, OUT>) userFunction)
                    .initEventTimeExtension(
                            getTimerService(), getEventTimeSupplier(), eventTimeWatermarkHandler);
        }

        this.userFunction.open(this.nonPartitionedContext);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestampFromStreamRecord(element);
        userFunction.processRecordFromNonBroadcastInput(
                element.getValue(), collector, partitionedContext);
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestampFromStreamRecord(element);
        userFunction.processRecordFromBroadcastInput(element.getValue(), nonPartitionedContext);
    }

    @Override
    public void processWatermark1Internal(WatermarkEvent watermark) throws Exception {
        WatermarkHandlingResult watermarkHandlingResultByUserFunction =
                userFunction.onWatermarkFromNonBroadcastInput(
                        watermark.getWatermark(), collector, nonPartitionedContext);
        if (watermarkHandlingResultByUserFunction == WatermarkHandlingResult.PEEK
                && watermarkDeclarationMap
                                .get(watermark.getWatermark().getIdentifier())
                                .getDefaultHandlingStrategy()
                        == WatermarkHandlingStrategy.FORWARD) {
            if (EventTimeExtensionImpl.isEventTimeExtensionWatermark(watermark.getWatermark())) {
                // if the watermark is event time related watermark, process them to advance event
                // time
                eventTimeWatermarkHandler.processWatermark(watermark.getWatermark(), 0);
            } else {
                output.emitWatermark(watermark);
            }
        }
    }

    @Override
    public void processWatermark2Internal(WatermarkEvent watermark) throws Exception {
        WatermarkHandlingResult watermarkHandlingResultByUserFunction =
                userFunction.onWatermarkFromBroadcastInput(
                        watermark.getWatermark(), collector, nonPartitionedContext);
        if (watermarkHandlingResultByUserFunction == WatermarkHandlingResult.PEEK
                && watermarkDeclarationMap
                                .get(watermark.getWatermark().getIdentifier())
                                .getDefaultHandlingStrategy()
                        == WatermarkHandlingStrategy.FORWARD) {
            if (EventTimeExtensionImpl.isEventTimeExtensionWatermark(watermark.getWatermark())) {
                // if the watermark is event time related watermark, process them to advance event
                // time
                eventTimeWatermarkHandler.processWatermark(watermark.getWatermark(), 1);
            } else {
                output.emitWatermark(watermark);
            }
        }
    }

    protected TimestampCollector<OUT> getOutputCollector() {
        return new OutputCollector<>(output);
    }

    protected NonPartitionedContext<OUT> getNonPartitionedContext() {
        return new DefaultNonPartitionedContext<>(
                context,
                partitionedContext,
                collector,
                false,
                null,
                output,
                watermarkDeclarationMap);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        // sanity check.
        checkState(inputId >= 1 && inputId <= 2);
        if (inputId == 1) {
            userFunction.endNonBroadcastInput(nonPartitionedContext);
        } else {
            userFunction.endBroadcastInput(nonPartitionedContext);
        }
    }

    protected Object currentKey() {
        throw new UnsupportedOperationException("The key is only defined for keyed operator");
    }

    protected BiConsumer<Runnable, Object> getProcessorWithKey() {
        if (isAsyncKeyOrderedProcessingEnabled()) {
            return (r, k) -> asyncProcessWithKey(k, r::run);
        } else {
            return (r, k) -> {
                Object oldKey = currentKey();
                try {
                    r.run();
                } finally {
                    setCurrentKey(oldKey);
                }
            };
        }
    }

    protected ProcessingTimeManager getProcessingTimeManager() {
        return UnsupportedProcessingTimeManager.INSTANCE;
    }

    @Override
    public void close() throws Exception {
        super.close();
        userFunction.close();
    }

    @Override
    public boolean isAsyncKeyOrderedProcessingEnabled() {
        // For non-keyed operators, we disable async state processing.
        return false;
    }

    protected InternalTimerService<VoidNamespace> getTimerService() {
        return null;
    }

    protected Supplier<Long> getEventTimeSupplier() {
        return () -> eventTimeWatermarkHandler.getLastEmitWatermark();
    }
}
