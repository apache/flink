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
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.TestingTransformation;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.transformations.DataStreamV2SinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.datastream.impl.stream.StreamTestUtils.assertProcessType;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NonKeyedPartitionStreamImpl}. */
class NonKeyedPartitionStreamImplTest {
    @Test
    void testPartitioning() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));
        stream.keyBy((data) -> 1).process(new StreamTestUtils.NoOpOneInputStreamProcessFunction());
        stream.shuffle().process(new StreamTestUtils.NoOpOneInputStreamProcessFunction());
        stream.broadcast()
                .connectAndProcess(
                        new NonKeyedPartitionStreamImpl<>(
                                env, new TestingTransformation<>("t2", Types.LONG, 1)),
                        new StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction());

        List<Transformation<?>> transformations = env.getTransformations();
        Transformation<?> keyedTransform = transformations.get(0).getInputs().get(0);
        assertThat(keyedTransform).isInstanceOf(PartitionTransformation.class);
        Transformation<?> shuffleTransform = transformations.get(1).getInputs().get(0);
        assertThat(shuffleTransform).isInstanceOf(PartitionTransformation.class);
        assertThat(transformations.get(2).getParallelism()).isOne();
    }

    @Test
    void testProcess() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));
        stream.process(new StreamTestUtils.NoOpOneInputStreamProcessFunction());
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(1);
        assertProcessType(transformations.get(0), OneInputTransformation.class, Types.LONG);
    }

    @Test
    void testProcessTwoOutput() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));
        NonKeyedPartitionStream.TwoNonKeyedPartitionStreams<Integer, Long> resultStream =
                stream.process(new StreamTestUtils.NoOpTwoOutputStreamProcessFunction());
        assertThat(resultStream.getFirst()).isInstanceOf(NonKeyedPartitionStream.class);
        assertThat(resultStream.getSecond()).isInstanceOf(NonKeyedPartitionStream.class);
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(1);
        assertProcessType(transformations.get(0), OneInputTransformation.class, Types.INT);
    }

    @Test
    void testConnectNonKeyedStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));
        stream.connectAndProcess(
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t2", Types.LONG, 1)),
                new StreamTestUtils.NoOpTwoInputNonBroadcastStreamProcessFunction());

        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(1);
        assertProcessType(transformations.get(0), TwoInputTransformation.class, Types.LONG);
    }

    @Test
    void testConnectBroadcastStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Long> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.LONG, 1));
        stream.connectAndProcess(
                new BroadcastStreamImpl<>(env, new TestingTransformation<>("t2", Types.INT, 1)),
                new StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction());

        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(1);
        assertProcessType(transformations.get(0), TwoInputTransformation.class, Types.LONG);
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
