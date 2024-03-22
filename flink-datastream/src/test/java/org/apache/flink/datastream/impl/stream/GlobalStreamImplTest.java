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

package org.apache.flink.datastream.impl.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.TestingTransformation;
import org.apache.flink.datastream.impl.stream.StreamTestUtils.NoOpOneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.stream.StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.stream.StreamTestUtils.NoOpTwoOutputStreamProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.transformations.DataStreamV2SinkTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlobalStreamImpl}. */
class GlobalStreamImplTest {
    @Test
    void testParallelism() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        GlobalStreamImpl<Integer> stream =
                new GlobalStreamImpl<>(env, new TestingTransformation<>("t1", Types.INT, 1));
        stream.process(new NoOpOneInputStreamProcessFunction());
        stream.process(new NoOpTwoOutputStreamProcessFunction());
        stream.connectAndProcess(
                new GlobalStreamImpl<>(env, new TestingTransformation<>("t2", Types.LONG, 1)),
                new StreamTestUtils.NoOpTwoInputNonBroadcastStreamProcessFunction());
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(3);
        assertThat(transformations.get(0).getParallelism()).isOne();
        assertThat(transformations.get(1).getParallelism()).isOne();
        assertThat(transformations.get(2).getParallelism()).isOne();
    }

    @Test
    void testPartitioning() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        GlobalStreamImpl<Integer> stream =
                new GlobalStreamImpl<>(env, new TestingTransformation<>("t1", Types.INT, 1));
        stream.keyBy((data) -> 1).process(new NoOpOneInputStreamProcessFunction());
        stream.shuffle().process(new NoOpOneInputStreamProcessFunction());
        stream.broadcast()
                .connectAndProcess(
                        new NonKeyedPartitionStreamImpl<>(
                                env, new TestingTransformation<>("t2", Types.LONG, 1)),
                        new NoOpTwoInputBroadcastStreamProcessFunction());

        List<Transformation<?>> transformations = env.getTransformations();
        Transformation<?> keyedTransform = transformations.get(0).getInputs().get(0);
        assertThat(keyedTransform).isInstanceOf(PartitionTransformation.class);
        Transformation<?> shuffleTransform = transformations.get(1).getInputs().get(0);
        assertThat(shuffleTransform).isInstanceOf(PartitionTransformation.class);
        assertThat(transformations.get(2).getParallelism()).isOne();
    }

    @Test
    void testToSink() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        GlobalStreamImpl<Integer> stream =
                new GlobalStreamImpl<>(env, new TestingTransformation<>("t1", Types.INT, 1));
        stream.toSink(DataStreamV2SinkUtils.wrapSink(new DiscardingSink<>()));
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations)
                .hasSize(1)
                .element(0)
                .isInstanceOf(DataStreamV2SinkTransformation.class);
    }
}
