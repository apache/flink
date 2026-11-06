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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property of {@link PipelineOptions#PARALLELISM_OVERRIDES} for both JobGraph generators: over
 * randomly generated programs, an override map changes the parallelism of exactly the overridden
 * job vertices and nothing else. Vertex ids, operator ids, chaining, names and edges are identical
 * to the translation without overrides, which is what keeps the map's keys and the operator ids of
 * keyed state valid across a rescale (FLINK-38770).
 */
abstract class ParallelismOverridesGeneratorTestBase {

    private static final int PROGRAMS_PER_MODE = 40;

    abstract JobGraph createJobGraph(StreamGraph streamGraph);

    private static Stream<Arguments> programs() {
        return LongStream.range(0, PROGRAMS_PER_MODE)
                .boxed()
                .flatMap(
                        seed ->
                                Stream.of(
                                        Arguments.of(seed, RuntimeExecutionMode.STREAMING),
                                        Arguments.of(seed, RuntimeExecutionMode.BATCH)));
    }

    @ParameterizedTest(name = "seed={0}, mode={1}")
    @MethodSource("programs")
    void testOverridesChangeOnlyTheParallelismOfOverriddenVertices(
            long seed, RuntimeExecutionMode mode) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(mode);
        buildRandomProgram(env, new Random(seed));

        final JobGraph reference = createJobGraph(env.getStreamGraph(false));
        final Map<JobVertexID, Integer> overrides =
                chooseOverrides(reference, new Random(seed + 1));

        final StreamGraph overriddenStreamGraph = env.getStreamGraph(false);
        overriddenStreamGraph
                .getJobConfiguration()
                .set(
                        PipelineOptions.PARALLELISM_OVERRIDES,
                        overrides.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                e -> e.getKey().toHexString(),
                                                e -> String.valueOf(e.getValue()))));
        final JobGraph overridden = createJobGraph(overriddenStreamGraph);

        assertSameTopology(reference, overridden);
        for (JobVertex vertex : overridden.getVertices()) {
            final JobVertex referenceVertex = reference.findVertexByID(vertex.getID());
            final Integer override = overrides.get(vertex.getID());
            if (override != null) {
                assertThat(vertex.getParallelism())
                        .as("overridden parallelism of %s", vertex.getName())
                        .isEqualTo(override);
                assertThat(vertex.isParallelismConfigured())
                        .as("%s counts as configured", vertex.getName())
                        .isTrue();
            } else {
                assertThat(vertex.getParallelism())
                        .as("parallelism of %s", vertex.getName())
                        .isEqualTo(referenceVertex.getParallelism());
            }
            assertThat(vertex.getMaxParallelism())
                    .as("max parallelism of %s", vertex.getName())
                    .isEqualTo(referenceVertex.getMaxParallelism());
        }
    }

    @Test
    void testValueThatIsNoParallelismFailsOnlyWhenItsVertexExists() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // chaining disabled so that the uid'd map is a chain head and its uid names a job vertex
        env.disableOperatorChaining();
        env.fromSequence(0, 10)
                .map(new Increment())
                .uid("bad-value")
                .sinkTo(new DiscardingSink<>());
        final JobVertexID vertexId =
                Arrays.stream(createJobGraph(env.getStreamGraph(false)).getVerticesAsArray())
                        .filter(
                                vertex ->
                                        vertex.getOperatorIDs().stream()
                                                .anyMatch(
                                                        pair ->
                                                                "bad-value"
                                                                        .equals(
                                                                                pair
                                                                                        .getUserDefinedOperatorUid())))
                        .map(JobVertex::getID)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("no vertex with a uid"));

        final StreamGraph unaffected = env.getStreamGraph(false);
        unaffected
                .getJobConfiguration()
                .set(
                        PipelineOptions.PARALLELISM_OVERRIDES,
                        Collections.singletonMap(new JobVertexID().toHexString(), "abc"));
        assertThat(createJobGraph(unaffected).getNumberOfVertices()).isGreaterThan(0);

        final StreamGraph affected = env.getStreamGraph(false);
        affected.getJobConfiguration()
                .set(
                        PipelineOptions.PARALLELISM_OVERRIDES,
                        Collections.singletonMap(vertexId.toHexString(), "abc"));
        assertThatThrownBy(() -> createJobGraph(affected))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(vertexId.toString())
                .hasMessageContaining("abc");
    }

    /** Everything but parallelism: ids, operators, names, invokables and edges. */
    static void assertSameTopology(JobGraph reference, JobGraph actual) {
        final Map<JobVertexID, JobVertex> referenceVertices = verticesById(reference);
        final Map<JobVertexID, JobVertex> actualVertices = verticesById(actual);
        assertThat(actualVertices.keySet())
                .as("job vertex ids")
                .containsExactlyInAnyOrderElementsOf(referenceVertices.keySet());
        for (JobVertexID id : referenceVertices.keySet()) {
            final JobVertex expected = referenceVertices.get(id);
            final JobVertex vertex = actualVertices.get(id);
            assertThat(vertex.getName()).isEqualTo(expected.getName());
            assertThat(vertex.getInvokableClassName()).isEqualTo(expected.getInvokableClassName());
            assertThat(generatedOperatorIds(vertex))
                    .as("chained operators of %s", expected.getName())
                    .isEqualTo(generatedOperatorIds(expected));
            assertThat(describeInputs(vertex))
                    .as("inputs of %s", expected.getName())
                    .isEqualTo(describeInputs(expected));
        }
    }

    private static Map<JobVertexID, JobVertex> verticesById(JobGraph jobGraph) {
        final Map<JobVertexID, JobVertex> vertices = new HashMap<>();
        for (JobVertex vertex : jobGraph.getVertices()) {
            vertices.put(vertex.getID(), vertex);
        }
        return vertices;
    }

    private static List<OperatorID> generatedOperatorIds(JobVertex vertex) {
        return vertex.getOperatorIDs().stream()
                .map(pair -> pair.getGeneratedOperatorID())
                .collect(Collectors.toList());
    }

    private static List<String> describeInputs(JobVertex vertex) {
        return vertex.getInputs().stream()
                .map(
                        edge ->
                                edge.getSource().getProducer().getID()
                                        + "|"
                                        + edge.getDistributionPattern()
                                        + "|forward="
                                        + edge.isForward()
                                        + "|"
                                        + edge.getSource().getResultType())
                .sorted()
                .collect(Collectors.toList());
    }

    private static Map<JobVertexID, Integer> chooseOverrides(JobGraph reference, Random random) {
        final Map<JobVertexID, Integer> overrides = new HashMap<>();
        final List<JobVertex> vertices = new ArrayList<>();
        reference.getVertices().forEach(vertices::add);
        for (JobVertex vertex : vertices) {
            if (random.nextBoolean()) {
                overrides.put(vertex.getID(), differentParallelism(vertex, random));
            }
        }
        if (overrides.isEmpty()) {
            final JobVertex vertex = vertices.get(random.nextInt(vertices.size()));
            overrides.put(vertex.getID(), differentParallelism(vertex, random));
        }
        return overrides;
    }

    /** An override equal to the current parallelism would not show whether it was applied. */
    private static int differentParallelism(JobVertex vertex, Random random) {
        int parallelism;
        do {
            parallelism = 1 + random.nextInt(4);
        } while (parallelism == vertex.getParallelism());
        return parallelism;
    }

    /**
     * A random program over {@code Long}s: one to three sources, a handful of chainable and
     * non-chainable steps (map, keyed map, rebalance, rescale, async, union, fork), random explicit
     * parallelism, random uids, random chaining breaks, one sink per open branch.
     */
    static void buildRandomProgram(StreamExecutionEnvironment env, Random random) {
        env.setParallelism(1 + random.nextInt(3));
        if (random.nextInt(4) == 0) {
            env.disableOperatorChaining();
        }
        final List<DataStream<Long>> open = new ArrayList<>();
        final int sources = 1 + random.nextInt(3);
        for (int i = 0; i < sources; i++) {
            final DataStreamSource<Long> source = env.fromSequence(0, 100);
            source.name("source-" + i);
            if (random.nextBoolean()) {
                source.setParallelism(1 + random.nextInt(3));
            }
            if (random.nextBoolean()) {
                source.uid("source-" + i);
            }
            open.add(source);
        }
        final int steps = 3 + random.nextInt(6);
        for (int step = 0; step < steps; step++) {
            int index = random.nextInt(open.size());
            final DataStream<Long> input = open.get(index);
            final SingleOutputStreamOperator<Long> output;
            switch (random.nextInt(7)) {
                case 0:
                    output = input.map(new Increment());
                    break;
                case 1:
                    output = input.keyBy(new Mod(5)).map(new Increment());
                    break;
                case 2:
                    output = input.rebalance().map(new Increment());
                    break;
                case 3:
                    output = input.rescale().map(new Increment());
                    break;
                case 4:
                    output =
                            AsyncDataStream.unorderedWait(
                                    input, new ImmediateAsync(), 1, TimeUnit.SECONDS);
                    break;
                case 5:
                    if (open.size() > 1) {
                        final int otherIndex =
                                (index + 1 + random.nextInt(open.size() - 1)) % open.size();
                        final DataStream<Long> other = open.remove(otherIndex);
                        if (otherIndex < index) {
                            index--;
                        }
                        output = input.union(other).map(new Increment());
                    } else {
                        output = input.map(new Increment());
                    }
                    break;
                default:
                    // fork: the input gets a second consumer, which exercises partition reuse
                    open.add(input.map(new Increment()).name("fork-" + step));
                    output = input.map(new Increment());
                    break;
            }
            output.name("op-" + step);
            if (random.nextInt(3) == 0) {
                output.setParallelism(1 + random.nextInt(3));
            }
            if (random.nextBoolean()) {
                output.uid("op-" + step);
            }
            if (random.nextInt(4) == 0) {
                output.disableChaining();
            }
            open.set(index, output);
        }
        int sinkIndex = 0;
        for (DataStream<Long> branch : open) {
            branch.sinkTo(new DiscardingSink<>()).name("sink-" + sinkIndex++);
        }
    }

    private static final class Increment implements MapFunction<Long, Long> {
        @Override
        public Long map(Long value) {
            return value + 1;
        }
    }

    private static final class Mod implements KeySelector<Long, Long> {
        private final long modulus;

        private Mod(long modulus) {
            this.modulus = modulus;
        }

        @Override
        public Long getKey(Long value) {
            return value % modulus;
        }
    }

    private static final class ImmediateAsync implements AsyncFunction<Long, Long> {
        @Override
        public void asyncInvoke(Long input, ResultFuture<Long> resultFuture) {
            resultFuture.complete(Collections.singleton(input));
        }
    }
}
