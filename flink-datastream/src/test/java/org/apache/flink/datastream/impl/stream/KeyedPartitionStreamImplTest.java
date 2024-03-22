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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.TestingTransformation;
import org.apache.flink.datastream.impl.stream.StreamTestUtils.NoOpOneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.stream.StreamTestUtils.NoOpTwoOutputStreamProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.transformations.DataStreamV2SinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.datastream.impl.stream.StreamTestUtils.assertProcessType;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyedPartitionStreamImpl}. */
public class KeyedPartitionStreamImplTest {
    @Test
    void testPartitioning() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        KeyedPartitionStream<Integer, Integer> stream = createKeyedStream(env);
        stream.keyBy((data) -> 1).process(new NoOpOneInputStreamProcessFunction());
        stream.shuffle().process(new NoOpOneInputStreamProcessFunction());
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
        KeyedPartitionStream<Integer, Integer> stream = createKeyedStream(env);
        stream.process(new NoOpOneInputStreamProcessFunction());
        KeyedPartitionStream<Integer, Long> resultStream =
                stream.process(new NoOpOneInputStreamProcessFunction(), Math::toIntExact);
        assertThat(resultStream).isInstanceOf(KeyedPartitionStream.class);
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(2);
        assertProcessType(transformations.get(0), OneInputTransformation.class, Types.LONG);
        assertProcessType(transformations.get(1), OneInputTransformation.class, Types.LONG);
    }

    @Test
    void testProcessTwoOutput() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        KeyedPartitionStream<Integer, Integer> stream = createKeyedStream(env);
        NonKeyedPartitionStream.TwoNonKeyedPartitionStreams<Integer, Long> resultStream1 =
                stream.process(new NoOpTwoOutputStreamProcessFunction());
        assertThat(resultStream1.getFirst()).isInstanceOf(NonKeyedPartitionStream.class);
        assertThat(resultStream1.getSecond()).isInstanceOf(NonKeyedPartitionStream.class);
        KeyedPartitionStream.TwoKeyedPartitionStreams<Integer, Integer, Long> resultStream2 =
                stream.process(new NoOpTwoOutputStreamProcessFunction(), x -> x, Math::toIntExact);
        assertThat(resultStream2.getFirst()).isInstanceOf(KeyedPartitionStream.class);
        assertThat(resultStream2.getSecond()).isInstanceOf(KeyedPartitionStream.class);
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(2);
        assertProcessType(transformations.get(0), OneInputTransformation.class, Types.INT);
        assertProcessType(transformations.get(1), OneInputTransformation.class, Types.INT);
    }

    @Test
    void testConnectKeyedStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        KeyedPartitionStream<Integer, Integer> stream = createKeyedStream(env);
        stream.connectAndProcess(
                createKeyedStream(
                        env,
                        new TestingTransformation<>("t2", Types.LONG, 1),
                        (KeySelector<Long, Integer>) Math::toIntExact),
                new StreamTestUtils.NoOpTwoInputNonBroadcastStreamProcessFunction());
        stream.connectAndProcess(
                createKeyedStream(
                        env,
                        new TestingTransformation<>("t3", Types.LONG, 1),
                        (KeySelector<Long, Integer>) Math::toIntExact),
                new StreamTestUtils.NoOpTwoInputNonBroadcastStreamProcessFunction(),
                (KeySelector<Long, Integer>) Math::toIntExact);

        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(2);
        assertProcessType(transformations.get(0), TwoInputTransformation.class, Types.LONG);
        assertProcessType(transformations.get(1), TwoInputTransformation.class, Types.LONG);
    }

    @Test
    void testConnectBroadcastStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        KeyedPartitionStream<Long, Long> stream =
                createKeyedStream(env, new TestingTransformation<>("t1", Types.LONG, 1), x -> x);
        BroadcastStreamImpl<Integer> s =
                new BroadcastStreamImpl<>(env, new TestingTransformation<>("t2", Types.INT, 1));
        stream.connectAndProcess(
                s, new StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction());
        stream.connectAndProcess(
                s, new StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction(), x -> x);
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(2);
        assertProcessType(transformations.get(0), TwoInputTransformation.class, Types.LONG);
        assertProcessType(transformations.get(1), TwoInputTransformation.class, Types.LONG);
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

    private static KeyedPartitionStream<Integer, Integer> createKeyedStream(
            ExecutionEnvironmentImpl env) {
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));
        return stream.keyBy(x -> x);
    }

    private static <OUT, KEY> KeyedPartitionStream<KEY, OUT> createKeyedStream(
            ExecutionEnvironmentImpl env,
            Transformation<OUT> transformation,
            KeySelector<OUT, KEY> keySelector) {
        NonKeyedPartitionStreamImpl<OUT> stream =
                new NonKeyedPartitionStreamImpl<>(env, transformation);
        return stream.keyBy(keySelector);
    }
}
