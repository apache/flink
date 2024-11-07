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

package org.apache.flink.state.api.output.operators;

import org.apache.flink.core.fs.Path;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * StateBootstrapWrapperOperator}.
 */
public class StateBootstrapWrapperOperatorFactory<
                IN,
                OUT,
                OP extends AbstractStreamOperator<OUT> & OneInputStreamOperator<IN, OUT>,
                OPF extends OneInputStreamOperatorFactory<IN, OUT>>
        extends AbstractStreamOperatorFactory<TaggedOperatorSubtaskState> {

    private final long timestamp;

    private final Path savepointPath;

    private Output<StreamRecord<TaggedOperatorSubtaskState>> output;

    private final WindowOperator<?, IN, ?, ?, ?> operator;

    public StateBootstrapWrapperOperatorFactory(
            long timestamp, Path savepointPath, WindowOperator<?, IN, ?, ?, ?> operator) {
        this.timestamp = timestamp;
        this.savepointPath = savepointPath;
        this.operator = operator;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<TaggedOperatorSubtaskState>> T createStreamOperator(
            StreamOperatorParameters<TaggedOperatorSubtaskState> parameters) {
        StateBootstrapWrapperOperator<IN, OUT, OP> wrapperOperator =
                new StateBootstrapWrapperOperator<>(timestamp, savepointPath, operator);
        wrapperOperator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        return (T) wrapperOperator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return StateBootstrapWrapperOperator.class;
    }

    private static class VoidOutput<T> implements Output<T> {

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void collect(T record) {}

        @Override
        public void close() {}
    }
}
