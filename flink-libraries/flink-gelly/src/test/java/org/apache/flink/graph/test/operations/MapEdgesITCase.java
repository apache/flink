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

package org.apache.flink.graph.test.operations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomParameterizedType;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomType;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/** Tests for {@link Graph#mapEdges}. */
@RunWith(Parameterized.class)
public class MapEdgesITCase extends MultipleProgramsTestBase {

    public MapEdgesITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String expectedResult;

    @Test
    public void testWithSameValue() throws Exception {
        /*
         * Test mapEdges() keeping the same value type
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, Long>> mappedEdges = graph.mapEdges(new AddOneMapper()).getEdges();
        List<Edge<Long, Long>> result = mappedEdges.collect();

        expectedResult =
                "1,2,13\n"
                        + "1,3,14\n"
                        + "2,3,24\n"
                        + "3,4,35\n"
                        + "3,5,36\n"
                        + "4,5,46\n"
                        + "5,1,52\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithStringValue() throws Exception {
        /*
         * Test mapEdges() and change the value type to String
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, String>> mappedEdges = graph.mapEdges(new ToStringMapper()).getEdges();
        List<Edge<Long, String>> result = mappedEdges.collect();

        expectedResult =
                "1,2,string(12)\n"
                        + "1,3,string(13)\n"
                        + "2,3,string(23)\n"
                        + "3,4,string(34)\n"
                        + "3,5,string(35)\n"
                        + "4,5,string(45)\n"
                        + "5,1,string(51)\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithTuple1Type() throws Exception {
        /*
         * Test mapEdges() and change the value type to a Tuple1
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, Tuple1<Long>>> mappedEdges =
                graph.mapEdges(new ToTuple1Mapper()).getEdges();
        List<Edge<Long, Tuple1<Long>>> result = mappedEdges.collect();

        expectedResult =
                "1,2,(12)\n"
                        + "1,3,(13)\n"
                        + "2,3,(23)\n"
                        + "3,4,(34)\n"
                        + "3,5,(35)\n"
                        + "4,5,(45)\n"
                        + "5,1,(51)\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithCustomType() throws Exception {
        /*
         * Test mapEdges() and change the value type to a custom type
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, DummyCustomType>> mappedEdges =
                graph.mapEdges(new ToCustomTypeMapper()).getEdges();
        List<Edge<Long, DummyCustomType>> result = mappedEdges.collect();

        expectedResult =
                "1,2,(T,12)\n"
                        + "1,3,(T,13)\n"
                        + "2,3,(T,23)\n"
                        + "3,4,(T,34)\n"
                        + "3,5,(T,35)\n"
                        + "4,5,(T,45)\n"
                        + "5,1,(T,51)\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithParametrizedCustomType() throws Exception {
        /*
         * Test mapEdges() and change the value type to a parameterized custom type
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, DummyCustomParameterizedType<Double>>> mappedEdges =
                graph.mapEdges(new ToCustomParametrizedTypeMapper()).getEdges();
        List<Edge<Long, DummyCustomParameterizedType<Double>>> result = mappedEdges.collect();

        expectedResult =
                "1,2,(12.0,12)\n"
                        + "1,3,(13.0,13)\n"
                        + "2,3,(23.0,23)\n"
                        + "3,4,(34.0,34)\n"
                        + "3,5,(35.0,35)\n"
                        + "4,5,(45.0,45)\n"
                        + "5,1,(51.0,51)\n";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    private static final class AddOneMapper implements MapFunction<Edge<Long, Long>, Long> {
        public Long map(Edge<Long, Long> edge) throws Exception {
            return edge.getValue() + 1;
        }
    }

    @SuppressWarnings("serial")
    private static final class ToStringMapper implements MapFunction<Edge<Long, Long>, String> {
        public String map(Edge<Long, Long> edge) throws Exception {
            return String.format("string(%d)", edge.getValue());
        }
    }

    @SuppressWarnings("serial")
    private static final class ToTuple1Mapper
            implements MapFunction<Edge<Long, Long>, Tuple1<Long>> {
        public Tuple1<Long> map(Edge<Long, Long> edge) throws Exception {
            Tuple1<Long> tupleValue = new Tuple1<>();
            tupleValue.setFields(edge.getValue());
            return tupleValue;
        }
    }

    @SuppressWarnings("serial")
    private static final class ToCustomTypeMapper
            implements MapFunction<Edge<Long, Long>, DummyCustomType> {
        public DummyCustomType map(Edge<Long, Long> edge) throws Exception {
            DummyCustomType dummyValue = new DummyCustomType();
            dummyValue.setIntField(edge.getValue().intValue());
            return dummyValue;
        }
    }

    @SuppressWarnings("serial")
    private static final class ToCustomParametrizedTypeMapper
            implements MapFunction<Edge<Long, Long>, DummyCustomParameterizedType<Double>> {

        public DummyCustomParameterizedType<Double> map(Edge<Long, Long> edge) throws Exception {
            DummyCustomParameterizedType<Double> dummyValue = new DummyCustomParameterizedType<>();
            dummyValue.setIntField(edge.getValue().intValue());
            dummyValue.setTField(new Double(edge.getValue()));
            return dummyValue;
        }
    }
}
