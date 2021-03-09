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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomParameterizedType;
import org.apache.flink.graph.utils.EdgeToTuple3Map;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Tests for {@link Graph#joinWithEdges}, {@link Graph#joinWithEdgesOnSource}, and {@link
 * Graph#joinWithEdgesOnTarget}.
 */
@RunWith(Parameterized.class)
public class JoinWithEdgesITCase extends MultipleProgramsTestBase {

    public JoinWithEdgesITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String expectedResult;

    @Test
    public void testWithEdgesInputDataset() throws Exception {
        /*
         * Test joinWithEdges with the input DataSet parameter identical
         * to the edge DataSet
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdges(
                        graph.getEdges().map(new EdgeToTuple3Map<>()), new AddValuesMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,26\n"
                        + "2,3,46\n"
                        + "3,4,68\n"
                        + "3,5,70\n"
                        + "4,5,90\n"
                        + "5,1,102\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithLessElements() throws Exception {
        /*
         * Test joinWithEdges with the input DataSet passed as a parameter containing
         * less elements than the edge DataSet, but of the same type
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdges(
                        graph.getEdges().first(3).map(new EdgeToTuple3Map<>()),
                        new AddValuesMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,26\n"
                        + "2,3,46\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithLessElementsDifferentType() throws Exception {
        /*
         * Test joinWithEdges with the input DataSet passed as a parameter containing
         * less elements than the edge DataSet and of a different type(Boolean)
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdges(
                        graph.getEdges().first(3).map(new BooleanEdgeValueMapper()),
                        new DoubleIfTrueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,26\n"
                        + "2,3,46\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithNoCommonKeys() throws Exception {
        /*
         * Test joinWithEdges with the input DataSet containing different keys than the edge DataSet
         * - the iterator becomes empty.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdges(
                        TestGraphUtils.getLongLongLongTuple3Data(env), new DoubleValueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,26\n"
                        + "2,3,46\n"
                        + "3,4,68\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithCustomType() throws Exception {
        /*
         * Test joinWithEdges with a DataSet containing custom parametrised type input values
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdges(
                        TestGraphUtils.getLongLongCustomTuple3Data(env), new CustomValueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,10\n"
                        + "1,3,20\n"
                        + "2,3,30\n"
                        + "3,4,40\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithEdgesOnSource() throws Exception {
        /*
         * Test joinWithEdgesOnSource with the input DataSet parameter identical
         * to the edge DataSet
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnSource(
                        graph.getEdges().map(new ProjectSourceAndValueMapper()),
                        new AddValuesMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,25\n"
                        + "2,3,46\n"
                        + "3,4,68\n"
                        + "3,5,69\n"
                        + "4,5,90\n"
                        + "5,1,102\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOnSourceWithLessElements() throws Exception {
        /*
         * Test joinWithEdgesOnSource with the input DataSet passed as a parameter containing
         * less elements than the edge DataSet, but of the same type
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnSource(
                        graph.getEdges().first(3).map(new ProjectSourceAndValueMapper()),
                        new AddValuesMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,25\n"
                        + "2,3,46\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOnSourceWithDifferentType() throws Exception {
        /*
         * Test joinWithEdgesOnSource with the input DataSet passed as a parameter containing
         * less elements than the edge DataSet and of a different type(Boolean)
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnSource(
                        graph.getEdges().first(3).map(new ProjectSourceWithTrueMapper()),
                        new DoubleIfTrueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,26\n"
                        + "2,3,46\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOnSourceWithNoCommonKeys() throws Exception {
        /*
         * Test joinWithEdgesOnSource with the input DataSet containing different keys than the edge DataSet
         * - the iterator becomes empty.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnSource(
                        TestGraphUtils.getLongLongTuple2SourceData(env), new DoubleValueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,20\n"
                        + "1,3,20\n"
                        + "2,3,60\n"
                        + "3,4,80\n"
                        + "3,5,80\n"
                        + "4,5,120\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOnSourceWithCustom() throws Exception {
        /*
         * Test joinWithEdgesOnSource with a DataSet containing custom parametrised type input values
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnSource(
                        TestGraphUtils.getLongCustomTuple2SourceData(env), new CustomValueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,10\n"
                        + "1,3,10\n"
                        + "2,3,30\n"
                        + "3,4,40\n"
                        + "3,5,40\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithEdgesOnTarget() throws Exception {
        /*
         * Test joinWithEdgesOnTarget with the input DataSet parameter identical
         * to the edge DataSet
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnTarget(
                        graph.getEdges().map(new ProjectTargetAndValueMapper()),
                        new AddValuesMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,26\n"
                        + "2,3,36\n"
                        + "3,4,68\n"
                        + "3,5,70\n"
                        + "4,5,80\n"
                        + "5,1,102\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testWithOnTargetWithLessElements() throws Exception {
        /*
         * Test joinWithEdgesOnTarget with the input DataSet passed as a parameter containing
         * less elements than the edge DataSet, but of the same type
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnTarget(
                        graph.getEdges().first(3).map(new ProjectTargetAndValueMapper()),
                        new AddValuesMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,26\n"
                        + "2,3,36\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOnTargetWithDifferentType() throws Exception {
        /*
         * Test joinWithEdgesOnTarget with the input DataSet passed as a parameter containing
         * less elements than the edge DataSet and of a different type(Boolean)
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnTarget(
                        graph.getEdges().first(3).map(new ProjectTargetWithTrueMapper()),
                        new DoubleIfTrueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,24\n"
                        + "1,3,26\n"
                        + "2,3,46\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOnTargetWithNoCommonKeys() throws Exception {
        /*
         * Test joinWithEdgesOnTarget with the input DataSet containing different keys than the edge DataSet
         * - the iterator becomes empty.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnTarget(
                        TestGraphUtils.getLongLongTuple2TargetData(env), new DoubleValueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,20\n"
                        + "1,3,40\n"
                        + "2,3,40\n"
                        + "3,4,80\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,140\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOnTargetWithCustom() throws Exception {
        /*
         * Test joinWithEdgesOnTarget with a DataSet containing custom parametrised type input values
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> res =
                graph.joinWithEdgesOnTarget(
                        TestGraphUtils.getLongCustomTuple2TargetData(env), new CustomValueMapper());

        DataSet<Edge<Long, Long>> data = res.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,10\n"
                        + "1,3,20\n"
                        + "2,3,20\n"
                        + "3,4,40\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    private static final class AddValuesMapper implements EdgeJoinFunction<Long, Long> {

        public Long edgeJoin(Long edgeValue, Long inputValue) throws Exception {
            return edgeValue + inputValue;
        }
    }

    @SuppressWarnings("serial")
    private static final class BooleanEdgeValueMapper
            implements MapFunction<Edge<Long, Long>, Tuple3<Long, Long, Boolean>> {
        public Tuple3<Long, Long, Boolean> map(Edge<Long, Long> edge) throws Exception {
            return new Tuple3<>(edge.getSource(), edge.getTarget(), true);
        }
    }

    @SuppressWarnings("serial")
    private static final class DoubleIfTrueMapper implements EdgeJoinFunction<Long, Boolean> {

        public Long edgeJoin(Long edgeValue, Boolean inputValue) {
            if (inputValue) {
                return edgeValue * 2;
            } else {
                return edgeValue;
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class DoubleValueMapper implements EdgeJoinFunction<Long, Long> {

        public Long edgeJoin(Long edgeValue, Long inputValue) {
            return inputValue * 2;
        }
    }

    @SuppressWarnings("serial")
    private static final class CustomValueMapper
            implements EdgeJoinFunction<Long, DummyCustomParameterizedType<Float>> {

        public Long edgeJoin(Long edgeValue, DummyCustomParameterizedType<Float> inputValue) {
            return (long) inputValue.getIntField();
        }
    }

    @SuppressWarnings("serial")
    private static final class ProjectSourceAndValueMapper
            implements MapFunction<Edge<Long, Long>, Tuple2<Long, Long>> {
        public Tuple2<Long, Long> map(Edge<Long, Long> edge) throws Exception {
            return new Tuple2<>(edge.getSource(), edge.getValue());
        }
    }

    @SuppressWarnings("serial")
    private static final class ProjectSourceWithTrueMapper
            implements MapFunction<Edge<Long, Long>, Tuple2<Long, Boolean>> {
        public Tuple2<Long, Boolean> map(Edge<Long, Long> edge) throws Exception {
            return new Tuple2<>(edge.getSource(), true);
        }
    }

    @SuppressWarnings("serial")
    private static final class ProjectTargetAndValueMapper
            implements MapFunction<Edge<Long, Long>, Tuple2<Long, Long>> {
        public Tuple2<Long, Long> map(Edge<Long, Long> edge) throws Exception {
            return new Tuple2<>(edge.getTarget(), edge.getValue());
        }
    }

    @SuppressWarnings("serial")
    private static final class ProjectTargetWithTrueMapper
            implements MapFunction<Edge<Long, Long>, Tuple2<Long, Boolean>> {
        public Tuple2<Long, Boolean> map(Edge<Long, Long> edge) throws Exception {
            return new Tuple2<>(edge.getTarget(), true);
        }
    }
}
