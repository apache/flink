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

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.state.IllegalRedistributionModeException;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.TestingTransformation;
import org.apache.flink.datastream.impl.utils.StreamUtils;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.transformations.DataStreamV2SinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.apache.flink.datastream.impl.stream.StreamTestUtils.assertProcessType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NonKeyedPartitionStreamImpl}. */
class NonKeyedPartitionStreamImplTest {

    private final StateDeclaration modeIdenticalStateDeclaration =
            StateDeclarations.listStateBuilder("list-state-identical", TypeDescriptors.INT)
                    .redistributeWithMode(StateDeclaration.RedistributionMode.IDENTICAL)
                    .build();

    private final StateDeclaration modeNoneStateDeclaration =
            StateDeclarations.listStateBuilder("list-state-none", TypeDescriptors.INT)
                    .redistributeWithMode(StateDeclaration.RedistributionMode.NONE)
                    .build();

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
    void testStateErrorWithOneInputStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));

        for (StateDeclaration stateDeclaration :
                Arrays.asList(modeNoneStateDeclaration, modeIdenticalStateDeclaration)) {
            assertThatThrownBy(
                            () ->
                                    stream.process(
                                            new StreamTestUtils.NoOpOneInputStreamProcessFunction(
                                                    new HashSet<>(
                                                            Collections.singletonList(
                                                                    stateDeclaration)))))
                    .isInstanceOf(IllegalRedistributionModeException.class);
        }
    }

    @Test
    void testStateErrorWithTwoOutputStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));

        for (StateDeclaration stateDeclaration :
                Arrays.asList(modeNoneStateDeclaration, modeIdenticalStateDeclaration)) {
            assertThatThrownBy(
                            () ->
                                    stream.process(
                                            new StreamTestUtils.NoOpTwoOutputStreamProcessFunction(
                                                    new HashSet<>(
                                                            Collections.singletonList(
                                                                    stateDeclaration)))))
                    .isInstanceOf(IllegalRedistributionModeException.class);
        }
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
    void testStateErrorWithConnectNonKeyedStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));

        for (StateDeclaration stateDeclaration1 :
                Arrays.asList(modeNoneStateDeclaration, modeIdenticalStateDeclaration)) {
            for (StateDeclaration stateDeclaration2 :
                    Arrays.asList(modeNoneStateDeclaration, modeIdenticalStateDeclaration)) {

                assertThatThrownBy(
                                () ->
                                        stream.connectAndProcess(
                                                new NonKeyedPartitionStreamImpl<>(
                                                        env,
                                                        new TestingTransformation<>(
                                                                "t2", Types.LONG, 1)),
                                                new StreamTestUtils
                                                        .NoOpTwoInputNonBroadcastStreamProcessFunction(
                                                        new HashSet<>(
                                                                Arrays.asList(
                                                                        stateDeclaration1,
                                                                        stateDeclaration2)))))
                        .isInstanceOf(IllegalRedistributionModeException.class);
            }
        }
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
    void testStateErrorWithConnectBroadcastStream() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Long> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.LONG, 1));

        assertThatThrownBy(
                        () ->
                                stream.connectAndProcess(
                                        new BroadcastStreamImpl<>(
                                                env,
                                                new TestingTransformation<>("t2", Types.INT, 1)),
                                        new StreamTestUtils
                                                .NoOpTwoInputBroadcastStreamProcessFunction(
                                                new HashSet<>(
                                                        Collections.singletonList(
                                                                modeNoneStateDeclaration)))))
                .isInstanceOf(IllegalRedistributionModeException.class);
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

    @Test
    void testConfig() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        NonKeyedPartitionStreamImpl<Integer> stream =
                new NonKeyedPartitionStreamImpl<>(
                        env, new TestingTransformation<>("t1", Types.INT, 1));
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer>
                configureHandle = StreamUtils.wrapWithConfigureHandle(stream);
        configureHandle.withName("test");
        configureHandle.withParallelism(2);
        configureHandle.withMaxParallelism(3);
        configureHandle.withUid("uid");
        org.apache.flink.api.common.SlotSharingGroup ssg =
                org.apache.flink.api.common.SlotSharingGroup.newBuilder("test-ssg").build();
        configureHandle.withSlotSharingGroup(ssg);
        Transformation<Integer> transformation = stream.getTransformation();
        assertThat(transformation.getName()).isEqualTo("test");
        assertThat(transformation.getParallelism()).isEqualTo(2);
        assertThat(transformation.getMaxParallelism()).isEqualTo(3);
        assertThat(transformation.getUid()).isEqualTo("uid");
        assertThat(transformation.getSlotSharingGroup()).hasValue(SlotSharingGroup.from(ssg));
    }
}
