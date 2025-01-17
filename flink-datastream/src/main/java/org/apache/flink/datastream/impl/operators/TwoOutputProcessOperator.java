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
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.datastream.impl.context.DefaultTwoOutputNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultTwoOutputPartitionedContext;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.datastream.impl.extension.eventtime.EventTimeExtensionImpl;
import org.apache.flink.datastream.impl.extension.eventtime.functions.EventTimeWrappedTwoOutputStreamProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateUdfStreamOperator;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermark.AbstractInternalWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.extension.eventtime.EventTimeWatermarkHandler;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Operator for {@link TwoOutputStreamProcessFunction}.
 *
 * <p>We support the second output via flink side-output mechanism.
 */
public class TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE>
        extends AbstractAsyncStateUdfStreamOperator<
                OUT_MAIN, TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE>>
        implements OneInputStreamOperator<IN, OUT_MAIN>, BoundedOneInput {
    protected transient TimestampCollector<OUT_MAIN> mainCollector;

    protected transient TimestampCollector<OUT_SIDE> sideCollector;

    protected transient DefaultRuntimeContext context;

    protected transient DefaultTwoOutputPartitionedContext<OUT_MAIN, OUT_SIDE> partitionedContext;

    protected transient TwoOutputNonPartitionedContext<OUT_MAIN, OUT_SIDE> nonPartitionedContext;

    protected OutputTag<OUT_SIDE> outputTag;

    protected transient Map<String, AbstractInternalWatermarkDeclaration<?>>
            watermarkDeclarationMap;

    // {@link EventTimeWatermarkHandler} will be used to process event time related watermarks
    protected transient EventTimeWatermarkHandler eventTimeWatermarkHandler;

    public TwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag) {
        super(userFunction);

        this.outputTag = outputTag;
    }

    @Override
    public void open() throws Exception {
        this.mainCollector = getMainCollector();
        this.sideCollector = getSideCollector();
        StreamingRuntimeContext operatorContext = getRuntimeContext();
        OperatorStateStore operatorStateStore = getOperatorStateBackend();
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
                new DefaultTwoOutputPartitionedContext<>(
                        context,
                        this::currentKey,
                        getProcessorWithKey(),
                        getProcessingTimeManager(),
                        operatorContext,
                        operatorStateStore);
        this.nonPartitionedContext = getNonPartitionedContext();
        this.partitionedContext.setNonPartitionedContext(nonPartitionedContext);
        this.eventTimeWatermarkHandler =
                new EventTimeWatermarkHandler(1, output, timeServiceManager);
        if (userFunction instanceof EventTimeWrappedTwoOutputStreamProcessFunction) {
            // note that the {@code initEventTimeExtension} in EventTimeWrappedProcessFunction
            // should be invoked before the {@code open}.
            ((EventTimeWrappedTwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE>) userFunction)
                    .initEventTimeExtension(
                            getTimerService(), getEventTimeSupplier(), eventTimeWatermarkHandler);
        }

        this.userFunction.open(this.nonPartitionedContext);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        mainCollector.setTimestampFromStreamRecord(element);
        sideCollector.setTimestampFromStreamRecord(element);
        userFunction.processRecord(
                element.getValue(), mainCollector, sideCollector, partitionedContext);
    }

    @Override
    public void processWatermarkInternal(WatermarkEvent watermark) throws Exception {
        WatermarkHandlingResult watermarkHandlingResultByUserFunction =
                userFunction.onWatermark(
                        watermark.getWatermark(),
                        mainCollector,
                        sideCollector,
                        nonPartitionedContext);
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
    public void endInput() throws Exception {
        userFunction.endInput(nonPartitionedContext);
    }

    protected TimestampCollector<OUT_MAIN> getMainCollector() {
        return new OutputCollector<>(output);
    }

    public TimestampCollector<OUT_SIDE> getSideCollector() {
        return new SideOutputCollector(output);
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
                try {
                    r.run();
                } finally {
                    setCurrentKey(oldKey);
                }
            };
        }
    }

    protected TwoOutputNonPartitionedContext<OUT_MAIN, OUT_SIDE> getNonPartitionedContext() {
        return new DefaultTwoOutputNonPartitionedContext<>(
                context,
                partitionedContext,
                mainCollector,
                sideCollector,
                false,
                null,
                output,
                watermarkDeclarationMap);
    }

    protected ProcessingTimeManager getProcessingTimeManager() {
        return UnsupportedProcessingTimeManager.INSTANCE;
    }

    /**
     * This is a special implementation of {@link TimestampCollector} that using side-output
     * mechanism to emit data.
     */
    protected class SideOutputCollector extends TimestampCollector<OUT_SIDE> {
        private final Output<StreamRecord<OUT_MAIN>> output;

        public SideOutputCollector(Output<StreamRecord<OUT_MAIN>> output) {
            this.output = output;
        }

        @Override
        public void collect(OUT_SIDE outputRecord) {
            output.collect(outputTag, reuse.replace(outputRecord));
        }

        @Override
        public void collectAndOverwriteTimestamp(OUT_SIDE record, long timestamp) {
            setTimestamp(timestamp);
            output.collect(outputTag, reuse.replace(record));
        }
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
