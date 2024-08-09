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
import org.apache.flink.api.common.watermark.TimestampWatermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermarks;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.watermark.WatermarkBuilder;
import org.apache.flink.test.util.MockExecutionEnvironment;
import org.apache.flink.test.util.WatermarkSupplier;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Integration test to check the source being able to send generalized watermarks. */
class SourceGeneralizedWatermarkITCase {

    private NonKeyedPartitionStream<Long> nonKeyedPartitionStream;
    private MockExecutionEnvironment env;

    @BeforeEach
    void setUp() throws ReflectiveOperationException {
        env = MockExecutionEnvironment.newInstance(new MockWatermarkSupplier(5));
        Set<WatermarkDeclaration> watermarkDeclarations =
                new HashSet<>(
                        Arrays.asList(
                                WatermarkBuilder.withLongWatermark()
                                        .withCompareSemantics(
                                                Watermarks.NumericWatermarkComparison.MIN)
                                        .build()));
        nonKeyedPartitionStream =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1L, 1L, 1L)),
                        "test-source",
                        watermarkDeclarations);
    }

    @Test
    void testSourceSendGeneralizedWatermark() throws Exception {
        nonKeyedPartitionStream
                .process(new CustomOneInputProcessFunction(true))
                .keyBy(new StatefulDataStreamV2ITCase.DefaultKeySelector())
                .process(new CustomOneInputProcessFunction(true));
        env.execute("dsv2 job");
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
            if (watermark instanceof TimestampWatermark) {
                ++receivedCustomWatermarks;
            }
        }

        @Override
        public WatermarkPolicy watermarkPolicy() {
            return watermark -> WatermarkPolicy.WatermarkResult.PEEK;
        }

        @Override
        public void close() {
            if (checkCustomWatermarks && receivedCustomWatermarks <= 0) {
                throw new FlinkRuntimeException(
                        "Expected to receive MockSourceWatermark but not received any");
            }
        }
    }

    public static class MockWatermarkSupplier implements WatermarkSupplier {
        private static final long serialVersionUID = 1L;
        private long counter = 0;
        private final long stopCounter;

        public MockWatermarkSupplier(long stopCounter) {
            this.stopCounter = stopCounter;
        }

        @Override
        public Optional<Watermark> getWatermark() {
            if (counter++ < stopCounter) {
                return Optional.of(new TimestampWatermark(counter++));
            } else {
                return Optional.empty();
            }
        }
    }
}
