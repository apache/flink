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
import org.apache.flink.api.common.WatermarkDeclaration;
import org.apache.flink.api.common.WatermarkPolicy;
import org.apache.flink.api.common.eventtime.GenericWatermark;
import org.apache.flink.datastream.api.context.EventTimeManager;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.WatermarkDeclarable;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.datastream.impl.context.UnsupportedEventTimeManager;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Set;

/** Operator for {@link OneInputStreamProcessFunction}. */
public class ProcessOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, OneInputStreamProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput, WatermarkDeclarable {

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
        partitionedContext =
                new DefaultPartitionedContext(
                        context, this::currentKey, this::setCurrentKey, getProcessingTimeManager(), getEventTimeManager());
        outputCollector = getOutputCollector();
        nonPartitionedContext = getNonPartitionedContext();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        outputCollector.setTimestampFromStreamRecord(element);
        userFunction.processRecord(element.getValue(), outputCollector, partitionedContext);
    }

    @Override
    public void processWatermark(WatermarkEvent mark) throws Exception {
        GenericWatermark genericWatermark = mark.getGenericWatermark();
        WatermarkPolicy watermarkPolicy = userFunction.watermarkPolicy();
        WatermarkPolicy.WatermarkResult watermarkResult = watermarkPolicy.useWatermark(genericWatermark);

        switch (watermarkResult) {
            case PEEK:
                super.processWatermark(mark);
                break;
            case POP:
                userFunction.onWatermark(mark.getGenericWatermark(), outputCollector, nonPartitionedContext);
                break;
            default:
                throw new FlinkRuntimeException("Unknown watermark result: " + watermarkResult);
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

    protected ProcessingTimeManager getProcessingTimeManager() {
        return UnsupportedProcessingTimeManager.INSTANCE;
    }

    protected EventTimeManager getEventTimeManager() {
        return UnsupportedEventTimeManager.INSTANCE;
    }

    protected NonPartitionedContext<OUT> getNonPartitionedContext() {
        return new DefaultNonPartitionedContext<>(
                context, partitionedContext, outputCollector, false, null, output);
    }

    @Override
    public Set<Class<? extends WatermarkDeclaration>> watermarkDeclarations() {
        return this.userFunction.watermarkDeclarations();
    }
}
