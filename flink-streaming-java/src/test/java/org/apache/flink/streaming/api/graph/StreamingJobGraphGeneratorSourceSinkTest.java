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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.util.TestExpandingSink;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests whether a generated job vertex is correctly marked as a source/sink by {@link
 * StreamingJobGraphGenerator}.
 */
@ExtendWith(TestLoggerExtension.class)
class StreamingJobGraphGeneratorSourceSinkTest {

    private StreamExecutionEnvironment env;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    void testLegacySource() {
        env.fromElements(0, 1).map(i -> i);

        final List<JobVertex> verticesSorted = getJobVertices();

        final JobVertex sourceVertex = verticesSorted.get(0);
        assertThat(sourceVertex.containsSources()).isTrue();
        assertThat(sourceVertex.containsSinks()).isFalse();
    }

    @Test
    void testNewSource() {
        env.fromSequence(0, 1).map(i -> i);

        final List<JobVertex> verticesSorted = getJobVertices();

        final JobVertex sourceVertex = verticesSorted.get(0);
        assertThat(sourceVertex.containsSources()).isTrue();
        assertThat(sourceVertex.containsSinks()).isFalse();
    }

    @Test
    void testMultiInputSource() {
        final DataStream<Long> source1 = env.fromSequence(0, 1);
        final DataStream<Long> source2 = env.fromSequence(0, 1);
        final MultipleInputTransformation<Long> multiInputTransform =
                new MultipleInputTransformation<>(
                        "multi-input-operator",
                        new StreamingJobGraphGeneratorTest.UnusedOperatorFactory(),
                        Types.LONG,
                        env.getParallelism());
        multiInputTransform.addInput(source1.map(i -> i).getTransformation());
        multiInputTransform.addInput(source2.getTransformation());
        multiInputTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
        env.addOperator(multiInputTransform);

        final List<JobVertex> verticesSorted = getJobVertices();

        final JobVertex source1Vertex = verticesSorted.get(0);
        assertThat(source1Vertex.containsSources()).isTrue();
        assertThat(source1Vertex.containsSinks()).isFalse();

        // source-2 is chained with the multi-input vertex
        final JobVertex multiInputVertex = verticesSorted.get(1);
        assertThat(multiInputVertex.containsSources()).isTrue();
        assertThat(multiInputVertex.containsSinks()).isFalse();
    }

    @Test
    void testLegacySink() {
        env.fromElements(0, 1).map(i -> i).startNewChain().addSink(new SinkFunction<Integer>() {});

        final List<JobVertex> verticesSorted = getJobVertices();

        final JobVertex sinkVertex = verticesSorted.get(1);
        assertThat(sinkVertex.containsSources()).isFalse();
        assertThat(sinkVertex.containsSinks()).isTrue();
    }

    @Test
    void testNewSink() {
        env.fromElements(0, 1).disableChaining().sinkTo(new TestExpandingSink());

        final List<JobVertex> verticesSorted = getJobVertices();

        final JobVertex sinkVertex = verticesSorted.get(1);
        assertThat(sinkVertex.containsSources()).isFalse();
        assertThat(sinkVertex.containsSinks()).isTrue();
    }

    @Test
    void testNewSinkWithSinkTopology() {
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.fromElements(0, 1).disableChaining().sinkTo(new TestExpandingSink());

        final List<JobVertex> verticesSorted = getJobVertices();

        final JobVertex sinkWriterVertex = verticesSorted.get(1);
        assertThat(sinkWriterVertex.containsSources()).isFalse();
        assertThat(sinkWriterVertex.containsSinks()).isTrue();

        final JobVertex sinkCommitterVertex = verticesSorted.get(2);
        assertThat(sinkCommitterVertex.containsSources()).isFalse();
        assertThat(sinkCommitterVertex.containsSinks()).isTrue();

        final JobVertex sinkPostCommitterVertex = verticesSorted.get(3);
        assertThat(sinkPostCommitterVertex.containsSources()).isFalse();
        assertThat(sinkPostCommitterVertex.containsSinks()).isTrue();
    }

    @Test
    void testChainedSourceSink() {
        env.setParallelism(1);
        env.fromElements(0, 1).sinkTo(new TestExpandingSink());

        final List<JobVertex> verticesSorted = getJobVertices();

        final JobVertex sourceSinkVertex = verticesSorted.get(0);
        assertThat(sourceSinkVertex.containsSources()).isTrue();
        assertThat(sourceSinkVertex.containsSinks()).isTrue();
    }

    private List<JobVertex> getJobVertices() {
        final StreamGraph streamGraph = env.getStreamGraph();
        final JobGraph jobGraph = streamGraph.getJobGraph();
        return jobGraph.getVerticesSortedTopologicallyFromSources();
    }
}
