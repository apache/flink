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
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the detection of the {@link RuntimeExecutionMode runtime execution mode} during stream
 * graph translation.
 */
public class StreamGraphGeneratorExecutionModeDetectionTest extends TestLogger {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testExecutionModePropagationFromEnvWithDefaultAndBoundedSource() {
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(100);

        environment
                .fromSource(
                        new MockSource(Boundedness.BOUNDED, 100),
                        WatermarkStrategy.noWatermarks(),
                        "bounded-source")
                .print();

        assertThat(
                environment.getStreamGraph(),
                hasProperties(
                        GlobalDataExchangeMode.ALL_EDGES_PIPELINED, ScheduleMode.EAGER, true));
    }

    @Test
    public void testExecutionModePropagationFromEnvWithDefaultAndUnboundedSource() {
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment
                .fromSource(
                        new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 100),
                        WatermarkStrategy.noWatermarks(),
                        "unbounded-source")
                .print();

        assertThat(
                environment.getStreamGraph(),
                hasProperties(
                        GlobalDataExchangeMode.ALL_EDGES_PIPELINED, ScheduleMode.EAGER, false));
    }

    @Test
    public void testExecutionModePropagationFromEnvWithAutomaticAndBoundedSource() {
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

        assertTrue(environment.isChainingEnabled());
        assertThat(environment.getCheckpointInterval(), is(equalTo(100L)));

        final StreamGraph streamGraph = environment.getStreamGraph();
        assertThat(
                streamGraph,
                hasProperties(
                        GlobalDataExchangeMode.FORWARD_EDGES_PIPELINED,
                        ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST,
                        false));
    }

    @Test
    public void testExecutionModePropagationFromEnvWithBatchAndUnboundedSource() {
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

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("combination is not allowed");
        environment.getStreamGraph();
    }

    @Test
    public void testDetectionThroughTransitivePredecessors() {
        final SourceTransformation<Integer, ?, ?> bounded =
                getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
        assertEquals(Boundedness.BOUNDED, bounded.getBoundedness());

        final SourceTransformation<Integer, ?, ?> unbounded =
                getSourceTransformation("Unbounded Source", Boundedness.CONTINUOUS_UNBOUNDED);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, unbounded.getBoundedness());

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
        assertThat(
                graph,
                hasProperties(
                        GlobalDataExchangeMode.ALL_EDGES_PIPELINED, ScheduleMode.EAGER, false));
    }

    @Test
    public void testBoundedDetection() {
        final SourceTransformation<Integer, ?, ?> bounded =
                getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
        assertEquals(Boundedness.BOUNDED, bounded.getBoundedness());

        final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, bounded);
        assertThat(
                graph,
                hasProperties(
                        GlobalDataExchangeMode.FORWARD_EDGES_PIPELINED,
                        ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST,
                        false));
    }

    @Test
    public void testUnboundedDetection() {
        final SourceTransformation<Integer, ?, ?> unbounded =
                getSourceTransformation("Unbounded Source", Boundedness.CONTINUOUS_UNBOUNDED);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, unbounded.getBoundedness());

        final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, unbounded);
        assertThat(
                graph,
                hasProperties(
                        GlobalDataExchangeMode.ALL_EDGES_PIPELINED, ScheduleMode.EAGER, false));
    }

    @Test
    public void testMixedDetection() {
        final SourceTransformation<Integer, ?, ?> unbounded =
                getSourceTransformation("Unbounded Source", Boundedness.CONTINUOUS_UNBOUNDED);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, unbounded.getBoundedness());

        final SourceTransformation<Integer, ?, ?> bounded =
                getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
        assertEquals(Boundedness.BOUNDED, bounded.getBoundedness());

        final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, unbounded);
        assertThat(
                graph,
                hasProperties(
                        GlobalDataExchangeMode.ALL_EDGES_PIPELINED, ScheduleMode.EAGER, false));
    }

    @Test
    public void testExplicitOverridesDetectedMode() {
        final SourceTransformation<Integer, ?, ?> bounded =
                getSourceTransformation("Bounded Source", Boundedness.BOUNDED);
        assertEquals(Boundedness.BOUNDED, bounded.getBoundedness());

        final StreamGraph graph = generateStreamGraph(RuntimeExecutionMode.AUTOMATIC, bounded);
        assertThat(
                graph,
                hasProperties(
                        GlobalDataExchangeMode.FORWARD_EDGES_PIPELINED,
                        ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST,
                        false));

        final StreamGraph streamingGraph =
                generateStreamGraph(RuntimeExecutionMode.STREAMING, bounded);
        assertThat(
                streamingGraph,
                hasProperties(
                        GlobalDataExchangeMode.ALL_EDGES_PIPELINED, ScheduleMode.EAGER, false));
    }

    private StreamGraph generateStreamGraph(
            final RuntimeExecutionMode initMode, final Transformation<?>... transformations) {

        final List<Transformation<?>> registeredTransformations = new ArrayList<>();
        Collections.addAll(registeredTransformations, transformations);

        StreamGraphGenerator streamGraphGenerator =
                new StreamGraphGenerator(
                        registeredTransformations, new ExecutionConfig(), new CheckpointConfig());
        streamGraphGenerator.setRuntimeExecutionMode(initMode);
        return streamGraphGenerator.generate();
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
            final GlobalDataExchangeMode exchangeMode,
            final ScheduleMode scheduleMode,
            final boolean isCheckpointingEnable) {

        return new TypeSafeMatcher<StreamGraph>() {
            @Override
            protected boolean matchesSafely(StreamGraph actualStreamGraph) {
                return exchangeMode == actualStreamGraph.getGlobalDataExchangeMode()
                        && scheduleMode == actualStreamGraph.getScheduleMode()
                        && actualStreamGraph.getCheckpointConfig().isCheckpointingEnabled()
                                == isCheckpointingEnable;
            }

            @Override
            public void describeTo(Description description) {
                description
                        .appendText("a StreamGraph with exchangeMode='")
                        .appendValue(exchangeMode)
                        .appendText("' and scheduleMode='")
                        .appendValue(scheduleMode)
                        .appendText("'");
            }

            @Override
            protected void describeMismatchSafely(
                    StreamGraph item, Description mismatchDescription) {
                mismatchDescription
                        .appendText("was ")
                        .appendText("a StreamGraph with exchangeMode='")
                        .appendValue(item.getGlobalDataExchangeMode())
                        .appendText("' and scheduleMode='")
                        .appendValue(item.getScheduleMode())
                        .appendText("'");
            }
        };
    }
}
