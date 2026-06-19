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

import org.apache.flink.api.common.state.IllegalRedistributionModeException;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.TestingTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.apache.flink.datastream.impl.stream.StreamTestUtils.assertProcessType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BroadcastStreamImpl}. */
class BroadcastStreamImplTest {

    private final StateDeclaration modeNoneStateDeclaration =
            StateDeclarations.listStateBuilder("list-state-none", TypeDescriptors.INT)
                    .redistributeWithMode(StateDeclaration.RedistributionMode.NONE)
                    .build();

    @Test
    void testConnectNonKeyedStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        BroadcastStreamImpl<Integer> stream =
                new BroadcastStreamImpl<>(env, new TestingTransformation<>("t1", Types.INT, 1));
        NonKeyedPartitionStreamImpl<Long> nonKeyedStream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t2", Types.LONG, 2));
        stream.connectAndProcess(
                nonKeyedStream, new StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction());
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(1);
        assertProcessType(transformations.get(0), TwoInputTransformation.class, Types.LONG);
    }

    @Test
    void testStateErrorWithConnectNonKeyedStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        BroadcastStreamImpl<Integer> stream =
                new BroadcastStreamImpl<>(env, new TestingTransformation<>("t1", Types.INT, 1));
        NonKeyedPartitionStreamImpl<Long> nonKeyedStream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t2", Types.LONG, 2));
        stream.connectAndProcess(
                nonKeyedStream, new StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction());

        assertThatThrownBy(
                        () ->
                                stream.connectAndProcess(
                                        nonKeyedStream,
                                        new StreamTestUtils
                                                .NoOpTwoInputBroadcastStreamProcessFunction(
                                                new HashSet<>(
                                                        Collections.singletonList(
                                                                modeNoneStateDeclaration)))))
                .isInstanceOf(IllegalRedistributionModeException.class);
    }

    @Test
    void testConnectKeyedStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        BroadcastStreamImpl<Integer> stream =
                new BroadcastStreamImpl<>(env, new TestingTransformation<>("t1", Types.INT, 1));
        NonKeyedPartitionStreamImpl<Long> nonKeyedStream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t2", Types.LONG, 2));
        stream.connectAndProcess(
                nonKeyedStream.keyBy(x -> x),
                new StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction());
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(1);
        assertProcessType(transformations.get(0), TwoInputTransformation.class, Types.LONG);
    }

    @Test
    void testStateErrorWithConnectKeyedStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        BroadcastStreamImpl<Integer> stream =
                new BroadcastStreamImpl<>(env, new TestingTransformation<>("t1", Types.INT, 1));
        NonKeyedPartitionStreamImpl<Long> nonKeyedStream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t2", Types.LONG, 2));

        Assertions.assertThatCode(
                        () ->
                                stream.connectAndProcess(
                                        nonKeyedStream.keyBy(x -> x),
                                        new StreamTestUtils
                                                .NoOpTwoInputBroadcastStreamProcessFunction(
                                                new HashSet<>(
                                                        Collections.singletonList(
                                                                modeNoneStateDeclaration)))))
                .doesNotThrowAnyException();
    }

    @Test
    void testConnectKeyedStreamWithOutputKeySelector() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        BroadcastStreamImpl<Integer> stream =
                new BroadcastStreamImpl<>(env, new TestingTransformation<>("t1", Types.INT, 1));
        NonKeyedPartitionStreamImpl<Long> nonKeyedStream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t2", Types.LONG, 2));
        KeyedPartitionStream<Long, Long> resultStream =
                stream.connectAndProcess(
                        nonKeyedStream.keyBy(x -> x),
                        new StreamTestUtils.NoOpTwoInputBroadcastStreamProcessFunction(),
                        x -> x);
        assertThat(resultStream).isInstanceOf(KeyedPartitionStream.class);
        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).hasSize(1);
        assertProcessType(transformations.get(0), TwoInputTransformation.class, Types.LONG);
    }
}
