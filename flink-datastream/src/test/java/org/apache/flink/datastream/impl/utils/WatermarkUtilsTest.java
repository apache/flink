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

package org.apache.flink.datastream.impl.utils;

import org.apache.flink.api.common.watermark.BoolWatermarkDeclaration;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkCombinationFunction;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.watermark.AbstractInternalWatermarkDeclaration;
import org.apache.flink.streaming.runtime.watermark.Alignable;
import org.apache.flink.streaming.runtime.watermark.AlignableLongWatermarkDeclaration;
import org.apache.flink.streaming.util.watermark.WatermarkUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link WatermarkUtils}. */
class WatermarkUtilsTest {

    private static List<WatermarkDeclaration> watermarkDeclarations;

    @BeforeAll
    static void setup() {
        watermarkDeclarations = new ArrayList<>();
        watermarkDeclarations.add(
                WatermarkDeclarations.newBuilder("watermark1")
                        .typeLong()
                        .combineFunctionMax()
                        .defaultHandlingStrategyIgnore()
                        .build());
        watermarkDeclarations.add(
                WatermarkDeclarations.newBuilder("watermark2")
                        .typeBool()
                        .combineFunctionOR()
                        .defaultHandlingStrategyIgnore()
                        .build());
        watermarkDeclarations.add(
                WatermarkDeclarations.newBuilder("watermark3")
                        .typeBool()
                        .combineFunctionAND()
                        .defaultHandlingStrategy(WatermarkHandlingStrategy.IGNORE)
                        .build());
        watermarkDeclarations.add(
                new AlignableLongWatermarkDeclaration(
                        "watermark4",
                        new WatermarkCombinationPolicy(
                                WatermarkCombinationFunction.NumericWatermarkCombinationFunction
                                        .MAX,
                                true),
                        WatermarkHandlingStrategy.IGNORE,
                        true));
        watermarkDeclarations.sort(Comparator.comparing(WatermarkDeclaration::getIdentifier));
    }

    @Test
    void testGetInternalWatermarkDeclarationsFromStreamGraph() throws ReflectiveOperationException {
        StreamGraph streamGraph = getStreamGraph(watermarkDeclarations);
        Set<AbstractInternalWatermarkDeclaration<?>> internalWatermarkDeclarationSet =
                WatermarkUtils.getInternalWatermarkDeclarationsFromStreamGraph(streamGraph);
        ArrayList<AbstractInternalWatermarkDeclaration<?>> internalWatermarkDeclarations =
                new ArrayList<>(internalWatermarkDeclarationSet);
        internalWatermarkDeclarations.sort(
                Comparator.comparing(WatermarkDeclaration::getIdentifier));
        assertThat(internalWatermarkDeclarations.size()).isEqualTo(watermarkDeclarations.size());
        for (int i = 0; i < internalWatermarkDeclarations.size(); i++) {
            judgeWatermarkDeclarationEqual(
                    watermarkDeclarations.get(i), internalWatermarkDeclarations.get(i));
        }
    }

    private static StreamGraph getStreamGraph(List<WatermarkDeclaration> watermarkDeclarations)
            throws ReflectiveOperationException {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1, 2, 3)), "custom-source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> stream1 =
                source.process(
                        new OneInputStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record,
                                    Collector<Integer> output,
                                    PartitionedContext<Integer> ctx)
                                    throws Exception {}

                            @Override
                            public Set<? extends WatermarkDeclaration> declareWatermarks() {
                                return Set.of(watermarkDeclarations.get(0));
                            }
                        });

        NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStream<Integer, Integer>
                stream2 =
                        stream1.process(
                                new TwoOutputStreamProcessFunction<Integer, Integer, Integer>() {
                                    @Override
                                    public void processRecord(
                                            Integer record,
                                            Collector<Integer> output1,
                                            Collector<Integer> output2,
                                            TwoOutputPartitionedContext<Integer, Integer> ctx)
                                            throws Exception {}

                                    @Override
                                    public Set<? extends WatermarkDeclaration> declareWatermarks() {
                                        return new HashSet<>(
                                                watermarkDeclarations.subList(
                                                        1, watermarkDeclarations.size()));
                                    }
                                });

        return ((ExecutionEnvironmentImpl) env).getStreamGraph();
    }

    private static void judgeWatermarkDeclarationEqual(
            WatermarkDeclaration watermarkDeclaration,
            AbstractInternalWatermarkDeclaration<?> internalWatermarkDeclaration) {
        assertThat(watermarkDeclaration.getIdentifier())
                .isEqualTo(internalWatermarkDeclaration.getIdentifier());
        boolean isAligned =
                (watermarkDeclaration instanceof Alignable)
                        && ((Alignable) watermarkDeclaration).isAligned();
        assertThat(isAligned).isEqualTo(internalWatermarkDeclaration.isAligned());
        if (watermarkDeclaration instanceof BoolWatermarkDeclaration) {
            BoolWatermarkDeclaration boolWatermarkDeclaration =
                    (BoolWatermarkDeclaration) watermarkDeclaration;
            assertThat(boolWatermarkDeclaration.getCombinationPolicy())
                    .isEqualTo(internalWatermarkDeclaration.getCombinationPolicy());
            assertThat(boolWatermarkDeclaration.getDefaultHandlingStrategy())
                    .isEqualTo(internalWatermarkDeclaration.getDefaultHandlingStrategy());
        } else if (watermarkDeclaration instanceof LongWatermarkDeclaration) {
            LongWatermarkDeclaration longWatermarkDeclaration =
                    (LongWatermarkDeclaration) watermarkDeclaration;
            assertThat(longWatermarkDeclaration.getCombinationPolicy())
                    .isEqualTo(internalWatermarkDeclaration.getCombinationPolicy());
            assertThat(longWatermarkDeclaration.getDefaultHandlingStrategy())
                    .isEqualTo(internalWatermarkDeclaration.getDefaultHandlingStrategy());
        } else {
            throw new IllegalArgumentException(
                    "Unknown WatermarkDeclaration type " + watermarkDeclaration.getClass());
        }
    }
}
