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
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.datastream.impl.extension.eventtime.EventTimeExtensionImpl;
import org.apache.flink.datastream.impl.extension.eventtime.functions.EventTimeWrappedOneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.eventtime.functions.ExtractEventTimeProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateUdfStreamOperator;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermark.AbstractInternalWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Operator for {@link OneInputStreamProcessFunction}. */
public class ProcessOperator<IN, OUT>
        extends AbstractAsyncStateUdfStreamOperator<OUT, OneInputStreamProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    protected transient DefaultRuntimeContext context;

    protected transient DefaultPartitionedContext<OUT> partitionedContext;

    protected transient NonPartitionedContext<OUT> nonPartitionedContext;

    protected transient TimestampCollector<OUT> outputCollector;

    protected transient Map<String, AbstractInternalWatermarkDeclaration<?>>
            watermarkDeclarationMap;

    // {@link EventTimeWatermarkHandler} will be used to process event time related watermarks
    protected transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public ProcessOperator(OneInputStreamProcessFunction<IN, OUT> userFunction) {
        super(userFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        StreamingRuntimeContext operatorContext = getRuntimeContext();
        TaskInfo taskInfo = operatorContext.getTaskInfo();
        context =
                new DefaultRuntimeContext(
                        operatorContext.getJobInfo().getJobName(),
                        operatorContext.getJobType(),
                        taskInfo.getNumberOfParallelSubtasks(),
                        taskInfo.getMaxNumberOfParallelSubtasks(),
                        taskInfo.getTaskName(),
                        taskInfo.getIndexOfThisSubtask(),
                        taskInfo.getAttemptNumber(),
                        operatorContext.getMetricGroup());

        outputCollector = getOutputCollector();
        watermarkDeclarationMap =
                config.getWatermarkDeclarations(getUserCodeClassloader()).stream()
                        .collect(
                                Collectors.toMap(
                                        AbstractInternalWatermarkDeclaration::getIdentifier,
                                        Function.identity()));

        partitionedContext =
                new DefaultPartitionedContext<>(
                        context,
                        this::currentKey,
                        getProcessorWithKey(),
                        getProcessingTimeManager(),
                        operatorContext,
                        getOperatorStateBackend());
        outputCollector = getOutputCollector();
        nonPartitionedContext = getNonPartitionedContext();
        partitionedContext.setNonPartitionedContext(nonPartitionedContext);
        this.eventTimeWatermarkHandler =
                new EventTimeWatermarkHandler(1, output, timeServiceManager);

        // Initialize event time extension related ProcessFunction
        if (userFunction instanceof ExtractEventTimeProcessFunction) {
            ((ExtractEventTimeProcessFunction<IN>) userFunction)
                    .initEventTimeExtension(
                            getExecutionConfig(),
                            partitionedContext.getNonPartitionedContext().getWatermarkManager(),
                            getProcessingTimeService());
        } else if (userFunction instanceof EventTimeWrappedOneInputStreamProcessFunction) {
            // note that the {@code initEventTimeExtension} in EventTimeWrappedProcessFunction
            // should be invoked before the {@code open}.
            ((EventTimeWrappedOneInputStreamProcessFunction<IN, OUT>) userFunction)
                    .initEventTimeExtension(
                            getTimerService(), getEventTimeSupplier(), eventTimeWatermarkHandler);
        }

        userFunction.open(nonPartitionedContext);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        outputCollector.setTimestampFromStreamRecord(element);
        userFunction.processRecord(element.getValue(), outputCollector, partitionedContext);
    }

    @Override
    public void processWatermarkInternal(WatermarkEvent watermark) throws Exception {
        WatermarkHandlingResult watermarkHandlingResultByUserFunction =
                userFunction.onWatermark(
                        watermark.getWatermark(), outputCollector, nonPartitionedContext);
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

    protected TimestampCollector<OUT> getOutputCollector() {
        return new OutputCollector<>(output);
    }

    @Override
    public void endInput() throws Exception {
        userFunction.endInput(nonPartitionedContext);
    }

    protected Object currentKey() {
        throw new UnsupportedOperationException("The key is only defined for keyed operator");
    }

    protected BiConsumer<Runnable, Object> getProcessorWithKey() {
        if (isAsyncStateProcessingEnabled()) {
            return (r, k) -> asyncProcessWithKey(k, r::run);
        } else {
            return (r, k) -> {
                Object oldKey = currentKey();
                setCurrentKey(k);
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

    protected NonPartitionedContext<OUT> getNonPartitionedContext() {
        return new DefaultNonPartitionedContext<>(
                context,
                partitionedContext,
                outputCollector,
                false,
                null,
                output,
                watermarkDeclarationMap);
    }

    @Override
    public void close() throws Exception {
        super.close();
        userFunction.close();
    }

    @Override
    public boolean isAsyncStateProcessingEnabled() {
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
