/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * A test harness for testing a {@link SourceOperator}.
 *
 * <p>This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements and
 * watermarks can be retrieved. you are free to modify these.
 */
public class SourceOperatorTestHarness<OUT> extends AbstractStreamOperatorTestHarness<OUT> {

    private final MockDataOutput<OUT> dataOutput;

    public SourceOperatorTestHarness(
            SourceOperatorFactory<OUT> operator, MockEnvironment environment) throws Exception {
        super(operator, environment);
        this.dataOutput = new MockDataOutput<>(new MockOutput());
    }

    public void emitNext() throws Exception {
        getCastedOperator().emitNext(dataOutput);
    }

    private SourceOperator<OUT, ?> getCastedOperator() {
        return (SourceOperator<OUT, ?>) operator;
    }

    private static class MockDataOutput<OUT> implements DataOutput<OUT> {
        private final Output<StreamRecord<OUT>> output;

        public MockDataOutput(Output<StreamRecord<OUT>> output) {
            this.output = output;
        }

        @Override
        public void emitRecord(StreamRecord<OUT> streamRecord) {
            output.collect(null, streamRecord);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            output.emitWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            output.emitWatermarkStatus(watermarkStatus);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            output.emitLatencyMarker(latencyMarker);
        }
    }
}
