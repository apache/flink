/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.util.ratelimit.DynamicRateLimitParams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link DataStreamSource}. */
class DataStreamSourceTest {

    /** Test constructor for new Sources (FLIP-27). */
    @Test
    void testConstructor() {
        int expectParallelism = 100;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MockSource mockSource = new MockSource(Boundedness.BOUNDED, 10);
        DataStreamSource<Integer> stream =
                env.fromSource(mockSource, WatermarkStrategy.noWatermarks(), "TestingSource");
        stream.setParallelism(expectParallelism);

        assertThat(stream.isParallel()).isTrue();
        assertThat(stream.getParallelism()).isEqualTo(expectParallelism);
    }

    @Test
    void testWithDynamicRateLimitParamsOverridesJobWideConfig() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MockSource mockSource = new MockSource(Boundedness.BOUNDED, 10);
        DataStreamSource<Integer> stream =
                env.fromSource(mockSource, WatermarkStrategy.noWatermarks(), "TestingSource");

        DynamicRateLimitParams override =
                new DynamicRateLimitParams(true, 5, 1000.0, 0.5, 1.5, 0.2, 0.5, 0.8);
        DataStreamSource<Integer> returned = stream.withDynamicRateLimitParams(override);

        assertThat(returned).isSameAs(stream);
        SourceTransformation<?, ?, ?> transformation =
                (SourceTransformation<?, ?, ?>) stream.getTransformation();
        assertThat(transformation.getDynamicRateLimitParams()).contains(override);
    }

    @Test
    void testWithDynamicRateLimitParamsRejectsLegacySources() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<Integer> sourceFunction =
                new SourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) {}

                    @Override
                    public void cancel() {}
                };
        DataStreamSource<Integer> legacyStream =
                new DataStreamSource<>(
                        env,
                        BasicTypeInfo.INT_TYPE_INFO,
                        new StreamSource<>(sourceFunction),
                        true,
                        "LegacyTestingSource");
        assertThat(legacyStream.getTransformation()).isInstanceOf(LegacySourceTransformation.class);

        assertThatThrownBy(
                        () ->
                                legacyStream.withDynamicRateLimitParams(
                                        DynamicRateLimitParams.DISABLED))
                .isInstanceOf(IllegalStateException.class);
    }
}
