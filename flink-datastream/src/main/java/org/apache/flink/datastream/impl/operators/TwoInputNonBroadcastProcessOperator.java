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
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.generalized.AbstractInternalWatermarkDeclaration;
import org.apache.flink.streaming.runtime.streamrecord.GeneralizedWatermarkElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Operator for {@link TwoInputNonBroadcastStreamProcessFunction}. */
public class TwoInputNonBroadcastProcessOperator<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<
                OUT, TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT>, BoundedMultiInput {

    protected transient TimestampCollector<OUT> collector;

    protected transient DefaultRuntimeContext context;

    protected transient DefaultPartitionedContext partitionedContext;

    protected transient NonPartitionedContext<OUT> nonPartitionedContext;

    protected transient Map<String, WatermarkHandlingStrategy> watermarkHandlingStrategyMap;

    public TwoInputNonBroadcastProcessOperator(
            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> userFunction) {
        super(userFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = getOutputCollector();
        StreamingRuntimeContext operatorContext = getRuntimeContext();
        OperatorStateBackend operatorStateBackend = getOperatorStateBackend();

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

        watermarkHandlingStrategyMap =
                config.getWatermarkDeclarations(getUserCodeClassloader()).stream()
                        .collect(
                                Collectors.toMap(
                                        AbstractInternalWatermarkDeclaration::getIdentifier,
                                        AbstractInternalWatermarkDeclaration
                                                ::getDefaultHandlingStrategy));
        partitionedContext =
                new DefaultPartitionedContext(
                        context,
                        this::currentKey,
                        this::setCurrentKey,
                        getProcessingTimeManager(),
                        operatorContext,
                        operatorStateBackend);
        nonPartitionedContext = getNonPartitionedContext();
        partitionedContext.setNonPartitionedContext(nonPartitionedContext);
        userFunction.open(nonPartitionedContext);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        collector.setTimestampFromStreamRecord(element);
        userFunction.processRecordFromFirstInput(element.getValue(), collector, partitionedContext);
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        collector.setTimestampFromStreamRecord(element);
        userFunction.processRecordFromSecondInput(
                element.getValue(), collector, partitionedContext);
    }

    @Override
    public void processGeneralizedWatermark1(GeneralizedWatermarkElement watermark)
            throws Exception {
        WatermarkHandlingResult watermarkHandlingResultByUserFunction =
                userFunction.onWatermarkFromFirstInput(
                        watermark.getWatermark(), collector, nonPartitionedContext);
        if (watermarkHandlingResultByUserFunction == WatermarkHandlingResult.PEEK
                && watermarkHandlingStrategyMap.get(watermark.getWatermark().getIdentifier())
                        == WatermarkHandlingStrategy.FORWARD) {
            output.emitGeneralizedWatermark(watermark);
        }
    }

    @Override
    public void processGeneralizedWatermark2(GeneralizedWatermarkElement watermark)
            throws Exception {
        WatermarkHandlingResult watermarkHandlingResultByUserFunction =
                userFunction.onWatermarkFromSecondInput(
                        watermark.getWatermark(), collector, nonPartitionedContext);
        if (watermarkHandlingResultByUserFunction == WatermarkHandlingResult.PEEK
                && watermarkHandlingStrategyMap.get(watermark.getWatermark().getIdentifier())
                        == WatermarkHandlingStrategy.FORWARD) {
            output.emitGeneralizedWatermark(watermark);
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
                watermarkHandlingStrategyMap.keySet());
    }

    @Override
    public void endInput(int inputId) throws Exception {
        // sanity check.
        checkState(inputId >= 1 && inputId <= 2);
        if (inputId == 1) {
            userFunction.endFirstInput(nonPartitionedContext);
        } else {
            userFunction.endSecondInput(nonPartitionedContext);
        }
    }

    protected Object currentKey() {
        throw new UnsupportedOperationException("The key is only defined for keyed operator");
    }

    protected ProcessingTimeManager getProcessingTimeManager() {
        return UnsupportedProcessingTimeManager.INSTANCE;
    }

    @Override
    public void close() throws Exception {
        super.close();
        userFunction.close();
    }
}
