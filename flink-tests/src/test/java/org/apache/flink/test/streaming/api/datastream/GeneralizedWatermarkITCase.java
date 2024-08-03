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

import org.apache.flink.api.common.WatermarkPolicy;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermarks;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.watermark.WatermarkBuilder;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
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
        Set<LongWatermarkDeclaration> watermarkDeclarations =
                new HashSet<>(
                        Arrays.asList(
                                WatermarkBuilder.withLongWatermark()
                                        .withCompareSemantics(
                                                Watermarks.NumericWatermarkComparison.MIN)
                                        .build()));

        nonKeyedPartitionStream
                .process(new CustomOneInputProcessFunction(false, watermarkDeclarations))
                .keyBy(new StatefulDataStreamV2ITCase.DefaultKeySelector())
                .process(new CustomOneInputProcessFunction(true, watermarkDeclarations));
        env.execute("dsv2 job");
    }

    static class CustomOneInputProcessFunction
            implements OneInputStreamProcessFunction<Long, Long> {
        private static final long serialVersionUID = 1L;

        private final Set<LongWatermarkDeclaration> watermarkDeclarations;

        private long receivedCustomWatermarks = 0;
        private final boolean checkCustomWatermarks;

        public CustomOneInputProcessFunction(
                boolean checkCustomWatermarks,
                Set<LongWatermarkDeclaration> watermarkDeclarations) {
            this.checkCustomWatermarks = checkCustomWatermarks;
            this.watermarkDeclarations = watermarkDeclarations;
        }

        @Override
        public void processRecord(Long record, Collector<Long> output, PartitionedContext ctx)
                throws Exception {
            output.collect(record + 10);
        }

        @Override
        public void onWatermark(
                Watermark watermark, Collector<Long> output, NonPartitionedContext<Long> ctx) {

            if (watermark instanceof LongWatermark) {
                ++receivedCustomWatermarks;
            }

            for (LongWatermarkDeclaration watermarkDeclaration : watermarkDeclarations) {
                ctx.getWatermarkManager().emitWatermark(watermarkDeclaration.createWatermark(999));
            }
        }

        @Override
        public WatermarkPolicy watermarkPolicy() {
            return watermark -> WatermarkPolicy.WatermarkResult.POP;
        }

        @Override
        public Set<? extends WatermarkDeclaration> watermarkDeclarations() {
            return watermarkDeclarations;
        }

        @Override
        public void close() {
            if (checkCustomWatermarks && receivedCustomWatermarks <= 0) {
                throw new FlinkRuntimeException(
                        "Expected to receive CustomWatermark but not received any");
            }
        }
    }
}
