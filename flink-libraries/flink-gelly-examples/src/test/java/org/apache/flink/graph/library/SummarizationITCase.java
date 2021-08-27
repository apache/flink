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

package org.apache.flink.graph.library;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.translate.TranslateEdgeValues;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.asm.translate.TranslateVertexValues;
import org.apache.flink.graph.asm.translate.translators.ToNullValue;
import org.apache.flink.graph.examples.data.SummarizationData;
import org.apache.flink.graph.library.Summarization.EdgeValue;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link Summarization}. */
@RunWith(Parameterized.class)
public class SummarizationITCase extends MultipleProgramsTestBase {

    private static final Pattern TOKEN_SEPARATOR = Pattern.compile(";");

    private static final Pattern ID_SEPARATOR = Pattern.compile(",");

    public SummarizationITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testWithVertexAndEdgeStringValues() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, String, String> input =
                Graph.fromDataSet(
                        SummarizationData.getVertices(env), SummarizationData.getEdges(env), env);

        List<Vertex<Long, Summarization.VertexValue<String>>> summarizedVertices =
                new ArrayList<>();
        List<Edge<Long, EdgeValue<String>>> summarizedEdges = new ArrayList<>();

        Graph<Long, Summarization.VertexValue<String>, EdgeValue<String>> output =
                input.run(new Summarization<>());

        output.getVertices().output(new LocalCollectionOutputFormat<>(summarizedVertices));
        output.getEdges().output(new LocalCollectionOutputFormat<>(summarizedEdges));

        env.execute();

        validateVertices(SummarizationData.EXPECTED_VERTICES, summarizedVertices);
        validateEdges(SummarizationData.EXPECTED_EDGES_WITH_VALUES, summarizedEdges);
    }

    @Test
    public void testWithVertexAndAbsentEdgeStringValues() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Graph<Long, String, NullValue> input =
                Graph.fromDataSet(
                                SummarizationData.getVertices(env),
                                SummarizationData.getEdges(env),
                                env)
                        .run(new TranslateEdgeValues<>(new ToNullValue<>()));

        List<Vertex<Long, Summarization.VertexValue<String>>> summarizedVertices =
                new ArrayList<>();
        List<Edge<Long, EdgeValue<NullValue>>> summarizedEdges = new ArrayList<>();

        Graph<Long, Summarization.VertexValue<String>, EdgeValue<NullValue>> output =
                input.run(new Summarization<>());

        output.getVertices().output(new LocalCollectionOutputFormat<>(summarizedVertices));
        output.getEdges().output(new LocalCollectionOutputFormat<>(summarizedEdges));

        env.execute();

        validateVertices(SummarizationData.EXPECTED_VERTICES, summarizedVertices);
        validateEdges(SummarizationData.EXPECTED_EDGES_ABSENT_VALUES, summarizedEdges);
    }

    @Test
    public void testWithVertexAndEdgeLongValues() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> input =
                Graph.fromDataSet(
                                SummarizationData.getVertices(env),
                                SummarizationData.getEdges(env),
                                env)
                        .run(new TranslateVertexValues<>(new StringToLong()))
                        .run(new TranslateEdgeValues<>(new StringToLong()));

        List<Vertex<Long, Summarization.VertexValue<Long>>> summarizedVertices = new ArrayList<>();
        List<Edge<Long, EdgeValue<Long>>> summarizedEdges = new ArrayList<>();

        Graph<Long, Summarization.VertexValue<Long>, EdgeValue<Long>> output =
                input.run(new Summarization<>());

        output.getVertices().output(new LocalCollectionOutputFormat<>(summarizedVertices));
        output.getEdges().output(new LocalCollectionOutputFormat<>(summarizedEdges));

        env.execute();

        validateVertices(SummarizationData.EXPECTED_VERTICES, summarizedVertices);
        validateEdges(SummarizationData.EXPECTED_EDGES_WITH_VALUES, summarizedEdges);
    }

    private <VV extends Comparable<VV>> void validateVertices(
            String[] expectedVertices,
            List<Vertex<Long, Summarization.VertexValue<VV>>> actualVertices) {
        Arrays.sort(expectedVertices);
        Collections.sort(
                actualVertices,
                new Comparator<Vertex<Long, Summarization.VertexValue<VV>>>() {
                    @Override
                    public int compare(
                            Vertex<Long, Summarization.VertexValue<VV>> o1,
                            Vertex<Long, Summarization.VertexValue<VV>> o2) {
                        int result = o1.getId().compareTo(o2.getId());
                        if (result == 0) {
                            result =
                                    o1.getValue()
                                            .getVertexGroupValue()
                                            .compareTo(o2.getValue().getVertexGroupValue());
                        }
                        return result;
                    }
                });

        for (int i = 0; i < expectedVertices.length; i++) {
            validateVertex(expectedVertices[i], actualVertices.get(i));
        }
    }

    private <EV extends Comparable<EV>> void validateEdges(
            String[] expectedEdges, List<Edge<Long, EdgeValue<EV>>> actualEdges) {
        Arrays.sort(expectedEdges);
        Collections.sort(
                actualEdges,
                new Comparator<Edge<Long, EdgeValue<EV>>>() {

                    @Override
                    public int compare(Edge<Long, EdgeValue<EV>> o1, Edge<Long, EdgeValue<EV>> o2) {
                        int result = o1.getSource().compareTo(o2.getSource());
                        if (result == 0) {
                            result = o1.getTarget().compareTo(o2.getTarget());
                        }
                        if (result == 0) {
                            result =
                                    o1.getValue()
                                            .getEdgeGroupValue()
                                            .compareTo(o2.getValue().getEdgeGroupValue());
                        }
                        return result;
                    }
                });

        for (int i = 0; i < expectedEdges.length; i++) {
            validateEdge(expectedEdges[i], actualEdges.get(i));
        }
    }

    private <VV> void validateVertex(
            String expected, Vertex<Long, Summarization.VertexValue<VV>> actual) {
        String[] tokens = TOKEN_SEPARATOR.split(expected);
        assertTrue(getListFromIdRange(tokens[0]).contains(actual.getId()));
        assertEquals(getGroupValue(tokens[1]), actual.getValue().getVertexGroupValue().toString());
        assertEquals(getGroupCount(tokens[1]), actual.getValue().getVertexGroupCount());
    }

    private <EV> void validateEdge(String expected, Edge<Long, EdgeValue<EV>> actual) {
        String[] tokens = TOKEN_SEPARATOR.split(expected);
        assertTrue(getListFromIdRange(tokens[0]).contains(actual.getSource()));
        assertTrue(getListFromIdRange(tokens[1]).contains(actual.getTarget()));
        assertEquals(getGroupValue(tokens[2]), actual.getValue().getEdgeGroupValue().toString());
        assertEquals(getGroupCount(tokens[2]), actual.getValue().getEdgeGroupCount());
    }

    private List<Long> getListFromIdRange(String idRange) {
        String[] split = ID_SEPARATOR.split(idRange);
        List<Long> result = new ArrayList<>(split.length);
        for (String id : split) {
            result.add(Long.parseLong(id));
        }
        return result;
    }

    private String getGroupValue(String token) {
        return ID_SEPARATOR.split(token)[0];
    }

    private Long getGroupCount(String token) {
        return Long.valueOf(ID_SEPARATOR.split(token)[1]);
    }

    private static class StringToLong implements TranslateFunction<String, Long> {

        @Override
        public Long translate(String value, Long reuse) throws Exception {
            return Long.parseLong(value);
        }
    }
}
