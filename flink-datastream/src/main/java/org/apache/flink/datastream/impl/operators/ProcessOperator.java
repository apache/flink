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

import org.apache.flink.api.common.GenericWatermarkPolicy;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.WatermarkCombiner;
import org.apache.flink.api.common.eventtime.GenericWatermark;
import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.datastream.impl.context.DefaultWatermarkManager;
import org.apache.flink.datastream.impl.context.UnsupportedProcessingTimeManager;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Operator for {@link OneInputStreamProcessFunction}. */
public class ProcessOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, OneInputStreamProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    protected transient DefaultRuntimeContext context;

    protected transient DefaultPartitionedContext partitionedContext;

    protected transient NonPartitionedContext<OUT> nonPartitionedContext;

    protected transient TimestampCollector<OUT> outputCollector;

    protected DefaultContext reusableWatermarkCombinerContext;

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
                        context, this::currentKey, this::setCurrentKey, getProcessingTimeManager());
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
        processWatermarkInternal(mark, 0, 1);
    }

    @Override
    public void processWatermark1(WatermarkEvent mark) throws Exception {
        processWatermarkInternal(mark, 0, 2);
    }

    @Override
    public void processWatermark2(WatermarkEvent mark) throws Exception {
        processWatermarkInternal(mark, 1, 2);
    }


    private void processWatermarkInternal(WatermarkEvent mark, int currentChannel, int numChannels) throws Exception {
        GenericWatermark genericWatermark = mark.getGenericWatermark();
        GenericWatermarkPolicy watermarkPolicy = userFunction.watermarkPolicy();
        GenericWatermarkPolicy.WatermarkResult watermarkResult = watermarkPolicy.useWatermark(genericWatermark);

        switch (watermarkResult) {
            case PEEK:
                super.processWatermark(mark);
            case POP:
                if (reusableWatermarkCombinerContext == null) {
                    reusableWatermarkCombinerContext = new DefaultContext(numChannels, currentChannel);
                } else {
                    reusableWatermarkCombinerContext.setChannelInfo(numChannels, currentChannel);
                }
                GenericWatermark combinedWatermark = watermarkPolicy.watermarkCombiner().combineWatermark(genericWatermark, reusableWatermarkCombinerContext);
                userFunction.onWatermark(combinedWatermark, outputCollector, nonPartitionedContext);
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

    protected NonPartitionedContext<OUT> getNonPartitionedContext() {
        return new DefaultNonPartitionedContext<>(
                context, partitionedContext, outputCollector, false, null, output);
    }

    private class DefaultContext implements WatermarkCombiner.Context {

        private int numChannels;
        private int currentChannel;

        public DefaultContext(int numChannels, int currentChannel) {
            this.numChannels = numChannels;
            this.currentChannel = currentChannel;
        }

        void setChannelInfo(int numChannels, int currentChannel) {
            this.numChannels = numChannels;
            this.currentChannel = currentChannel;
        }

        @Override
        public int getNumberOfInputChannels() {
            return numChannels;
        }

        @Override
        public int getIndexOfCurrentChannel() {
            return currentChannel;
        }
    }
}
