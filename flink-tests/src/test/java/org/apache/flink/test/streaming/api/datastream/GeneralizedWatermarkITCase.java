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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.WatermarkCombiner;
import org.apache.flink.api.common.WatermarkDeclaration;
import org.apache.flink.api.common.WatermarkOutput;
import org.apache.flink.api.common.WatermarkPolicy;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/** Integration test for state access and usage of DataStream V2. */
class GeneralizedWatermarkITCase {

    private NonKeyedPartitionStream<Long> nonKeyedPartitionStream;
    private ExecutionEnvironment env;

    @BeforeEach
    void setUp() throws ReflectiveOperationException {
        env = ExecutionEnvironment.getInstance();
        nonKeyedPartitionStream =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1L, 1L, 1L)), "test-source");
    }

    @Test
    void testCustomGeneralizedWatermarksAreSentAndReceived() throws Exception {
        nonKeyedPartitionStream
                .process(new CustomOneInputProcessFunction(false))
                .keyBy(new StatefulDataStreamV2ITCase.DefaultKeySelector())
                .process(new CustomOneInputProcessFunction(true));
        env.execute("dsv2 job");
    }

    public static class CustomWatermark implements Watermark {
        String strPayload;

        public CustomWatermark(String strPayload) {
            this.strPayload = strPayload;
        }

        public String getStrPayload() {
            return strPayload;
        }
    }

    static class CustomOneInputProcessFunction
            implements OneInputStreamProcessFunction<Long, Long> {
        private static final long serialVersionUID = 1L;

        private long receivedCustomWatermarks = 0;
        private final boolean checkCustomWatermarks;

        public CustomOneInputProcessFunction(boolean checkCustomWatermarks) {
            this.checkCustomWatermarks = checkCustomWatermarks;
        }

        @Override
        public void processRecord(Long record, Collector<Long> output, PartitionedContext ctx)
                throws Exception {
            output.collect(record + 10);
        }

        @Override
        public void onWatermark(
                Watermark watermark, Collector<Long> output, NonPartitionedContext<Long> ctx) {

            if (watermark instanceof CustomWatermark) {
                ++receivedCustomWatermarks;
            }

            ctx.getWatermarkManager()
                    .emitWatermark(
                            new CustomWatermark("Override time: " + System.currentTimeMillis()));
        }

        @Override
        public WatermarkPolicy watermarkPolicy() {
            return watermark -> WatermarkPolicy.WatermarkResult.POP;
        }

        @Override
        public Set<Class<? extends WatermarkDeclaration>> watermarkDeclarations() {
            return Collections.singleton(CustomWatermarkDeclaration.class);
        }

        @Override
        public void close() {
            if (checkCustomWatermarks && receivedCustomWatermarks <= 0) {
                throw new FlinkRuntimeException(
                        "Expected to receive CustomWatermark but not received any");
            }
        }
    }

    public static class CustomWatermarkDeclaration implements WatermarkDeclaration {

        @Override
        public WatermarkSerde declaredWatermark() {
            return new WatermarkSerde() {
                @Override
                public Class<? extends Watermark> watermarkClass() {
                    return CustomWatermark.class;
                }

                @Override
                public void serialize(Watermark genericWatermark, DataOutputView target)
                        throws IOException {
                    target.writeUTF(((CustomWatermark) genericWatermark).getStrPayload());
                }

                @Override
                public Watermark deserialize(DataInputView inputView) throws IOException {
                    return new CustomWatermark(inputView.readUTF());
                }
            };
        }

        @Override
        public WatermarkCombiner watermarkCombiner() {
            return new WatermarkCombiner() {
                @Override
                public void combineWatermark(
                        Watermark watermark, Context context, WatermarkOutput output)
                        throws Exception {
                    if (!(watermark instanceof CustomWatermark)) {
                        throw new FlinkRuntimeException(
                                "Expected CustomWatermark, got " + watermark.getClass());
                    }
                    // custom watermark alignment logic
                    if (context.getIndexOfCurrentChannel() == 0) {
                        output.emitWatermark(watermark);
                    }
                }
            };
        }
    }
}
