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
import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.api.watermark.WatermarkPolicy;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.GeneralizedWatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Operator for {@link OneInputStreamProcessFunction}. */
public class ProcessOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, OneInputStreamProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    protected transient DefaultRuntimeContext context;

    protected transient DefaultPartitionedContext partitionedContext;

    protected transient NonPartitionedContext<OUT> nonPartitionedContext;

    protected transient TimestampCollector<OUT> outputCollector;

    public ProcessOperator(OneInputStreamProcessFunction<IN, OUT> userFunction) {
        super(userFunction);

        chainingStrategy = ChainingStrategy.ALWAYS;
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
                        operatorContext.getMetricGroup());
        outputCollector = getOutputCollector();
        nonPartitionedContext = getNonPartitionedContext();
        partitionedContext =
                new DefaultPartitionedContext(
                        context,
                        this::currentKey,
                        this::setCurrentKey,
                        getProcessingTimeManager(),
                        operatorContext,
                        getOperatorStateBackend(),
                        nonPartitionedContext);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        outputCollector.setTimestampFromStreamRecord(element);
        userFunction.processRecord(element.getValue(), outputCollector, partitionedContext);
    }

    @Override
    public void processGeneralizedWatermark(GeneralizedWatermarkEvent watermark) throws Exception {
        GeneralizedWatermark genericWatermark = watermark.getWatermark();
        WatermarkPolicy watermarkPolicy =
                userFunction.onWatermark(genericWatermark, outputCollector, nonPartitionedContext);
        if (watermarkPolicy == WatermarkPolicy.PEEK) {
            output.emitGeneralizedWatermark(watermark);
        }
    }

    //    @Override
    //    public void processWatermark(Watermark mark) throws Exception {
    //        Watermark genericWatermark = mark.getWatermark();
    //        WatermarkPolicy watermarkPolicy = userFunction.watermarkPolicy();
    //        WatermarkPolicy.WatermarkResult watermarkResult =
    //                watermarkPolicy.useWatermark(genericWatermark);
    //        if (timeServiceManager != null) {
    //            timeServiceManager.advanceWatermark(mark);
    //        }
    //        switch (watermarkResult) {
    //            case PEEK:
    //                userFunction.onWatermark(
    //                        mark.getWatermark(), outputCollector, limitedNonPartitionedContext);
    //                super.processWatermark(mark);
    //                break;
    //            case POP:
    //                userFunction.onWatermark(
    //                        mark.getWatermark(), outputCollector, nonPartitionedContext);
    //                break;
    //            default:
    //                throw new FlinkRuntimeException("Unknown watermark result: " +
    // watermarkResult);
    //        }
    //    }

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

    protected ProcessingTimeManager getProcessingTimeManager() {
        return UnsupportedProcessingTimeManager.INSTANCE;
    }

    protected NonPartitionedContext<OUT> getNonPartitionedContext() {
        return new DefaultNonPartitionedContext<>(
                context, partitionedContext, outputCollector, false, null, output);
    }

    @Override
    public void close() throws Exception {
        super.close();
        userFunction.close();
    }
}
