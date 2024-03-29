/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.HamcrestCondition.matching;

/**
 * Tests for the detection of the {@link RuntimeExecutionMode runtime execution mode} during stream
 * graph translation.
 */
class StreamGraphGeneratorExecutionModeDetectionTest {

    @Test
    void testExecutionModePropagationFromEnvWithDefaultAndBoundedSource() {
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(100);

        environment
                .fromSource(
                        new MockSource(Boundedness.BOUNDED, 100),
                        WatermarkStrategy.noWatermarks(),
                        "bounded-source")
                .print();

        assertThat(environment.getStreamGraph())
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                                        JobType.STREAMING,
                                        true,
                                        true)));
    }

    @Test
    void testExecutionModePropagationFromEnvWithDefaultAndUnboundedSource() {
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment
                .fromSource(
                        new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 100),
                        WatermarkStrategy.noWatermarks(),
                        "unbounded-source")
                .print();

        assertThat(environment.getStreamGraph())
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                                        JobType.STREAMING,
                                        false,
                                        true)));
    }

    @Test
    void testExecutionModePropagationFromEnvWithAutomaticAndBoundedSource() {
        final Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.AUTOMATIC);

        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(100L);
        environment.configure(config, getClass().getClassLoader());

        environment
                .fromSource(
                        new MockSource(Boundedness.BOUNDED, 100),
                        WatermarkStrategy.noWatermarks(),
                        "bounded-source")
                .print();

        assertThat(environment.isChainingEnabled()).isTrue();
        assertThat(environment.getCheckpointInterval()).isEqualTo(100L);

        final StreamGraph streamGraph = environment.getStreamGraph();
        assertThat(streamGraph)
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_BLOCKING,
                                        JobType.BATCH,
                                        false,
                                        false)));
    }

    @Test
    void testExecutionModePropagationFromEnvWithBatchAndUnboundedSource() {
        final Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.configure(config, getClass().getClassLoader());

        environment
                .fromSource(
                        new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 100),
                        WatermarkStrategy.noWatermarks(),
                        "unbounded-source")
                .print();

        assertThatThrownBy(environment::getStreamGraph)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("combination is not allowed");
    }

    @Test
    void testDetectionThroughTransitivePredecessors() {
        final SourceTransformation<Integer, ?, ?> bounded =
                getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
        assertThat(bounded.getBoundedness()).isEqualTo(Boundedness.BOUNDED);

        final SourceTransformation<Integer, ?, ?> unbounded =
                getSourceTransformation("Unbounded Source", Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(unbounded.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);

        final TwoInputTransformation<Integer, Integer, Integer> resultTransform =
                new TwoInputTransformation<>(
                        bounded,
                        unbounded,
                        "Test Two Input Transformation",
                        SimpleOperatorFactory.of(
                                new StreamGraphGeneratorTest
                                        .OutputTypeConfigurableOperationWithTwoInputs()),
                        BasicTypeInfo.INT_TYPE_INFO,
                        1);

        final StreamGraph graph =
                generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, resultTransform);
        assertThat(graph)
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                                        JobType.STREAMING,
                                        false,
                                        true)));
    }

    @Test
    void testBoundedDetection() {
        final SourceTransformation<Integer, ?, ?> bounded =
                getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
        assertThat(bounded.getBoundedness()).isEqualTo(Boundedness.BOUNDED);

        final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, bounded);
        assertThat(graph)
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_BLOCKING,
                                        JobType.BATCH,
                                        false,
                                        false)));
    }

    @Test
    void testUnboundedDetection() {
        final SourceTransformation<Integer, ?, ?> unbounded =
                getSourceTransformation("Unbounded Source", Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(unbounded.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);

        final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, unbounded);
        assertThat(graph)
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                                        JobType.STREAMING,
                                        false,
                                        true)));
    }

    @Test
    void testMixedDetection() {
        final SourceTransformation<Integer, ?, ?> unbounded =
                getSourceTransformation("Unbounded Source", Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(unbounded.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);

        final SourceTransformation<Integer, ?, ?> bounded =
                getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
        assertThat(bounded.getBoundedness()).isEqualTo(Boundedness.BOUNDED);

        final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, unbounded);
        assertThat(graph)
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                                        JobType.STREAMING,
                                        false,
                                        true)));
    }

    @Test
    void testExplicitOverridesDetectedMode() {
        final SourceTransformation<Integer, ?, ?> bounded =
                getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
        assertThat(bounded.getBoundedness()).isEqualTo(Boundedness.BOUNDED);

        final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, bounded);
        assertThat(graph)
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_BLOCKING,
                                        JobType.BATCH,
                                        false,
                                        false)));

        final StreamGraph streamingGraph =
                generateStreamGraph(RuntimeExecutionMode.STREAMING, bounded);
        assertThat(streamingGraph)
                .is(
                        matching(
                                hasProperties(
                                        GlobalStreamExchangeMode.ALL_EDGES_PIPELINED,
                                        JobType.STREAMING,
                                        false,
                                        true)));
    }

    private StreamGraph generateStreamGraph(
            final RuntimeExecutionMode initMode, final Transformation<?>... transformations) {

        final List<Transformation<?>> registeredTransformations = new ArrayList<>();
        Collections.addAll(registeredTransformations, transformations);

        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, initMode);

        return new StreamGraphGenerator(
                        registeredTransformations,
                        new ExecutionConfig(),
                        new CheckpointConfig(),
                        configuration)
                .generate();
    }

    private SourceTransformation<Integer, ?, ?> getSourceTransformation(
            final String name, final Boundedness boundedness) {
        return new SourceTransformation<>(
                name,
                new MockSource(boundedness, 100),
                WatermarkStrategy.noWatermarks(),
                IntegerTypeInfo.of(Integer.class),
                1);
    }

    private static TypeSafeMatcher<StreamGraph> hasProperties(
            final GlobalStreamExchangeMode exchangeMode,
            final JobType jobType,
            final boolean isCheckpointingEnabled,
            final boolean isAllVerticesInSameSlotSharingGroupByDefault) {

        return new TypeSafeMatcher<StreamGraph>() {
            @Override
            protected boolean matchesSafely(StreamGraph actualStreamGraph) {
                return exchangeMode == actualStreamGraph.getGlobalStreamExchangeMode()
                        && jobType == actualStreamGraph.getJobType()
                        && actualStreamGraph.getCheckpointConfig().isCheckpointingEnabled()
                                == isCheckpointingEnabled
                        && actualStreamGraph.isAllVerticesInSameSlotSharingGroupByDefault()
                                == isAllVerticesInSameSlotSharingGroupByDefault;
            }

            @Override
            public void describeTo(Description description) {
                description
                        .appendText("a StreamGraph with exchangeMode=")
                        .appendValue(exchangeMode)
                        .appendText(", jobType=")
                        .appendValue(jobType)
                        .appendText(", isCheckpointingEnabled=")
                        .appendValue(isCheckpointingEnabled)
                        .appendText(", isAllVerticesInSameSlotSharingGroupByDefault=")
                        .appendValue(isAllVerticesInSameSlotSharingGroupByDefault);
            }

            @Override
            protected void describeMismatchSafely(
                    StreamGraph item, Description mismatchDescription) {
                mismatchDescription
                        .appendText("was ")
                        .appendText("a StreamGraph with exchangeMode=")
                        .appendValue(exchangeMode)
                        .appendText(", jobType=")
                        .appendValue(jobType)
                        .appendText(", isCheckpointingEnabled=")
                        .appendValue(isCheckpointingEnabled)
                        .appendText(", isAllVerticesInSameSlotSharingGroupByDefault=")
                        .appendValue(isAllVerticesInSameSlotSharingGroupByDefault);
            }
        };
    }
}
