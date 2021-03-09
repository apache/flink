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

package org.apache.flink.graph.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.translate.Translate;
import org.apache.flink.graph.asm.translate.translators.LongToLongValue;
import org.apache.flink.graph.examples.data.ConnectedComponentsDefaultData;
import org.apache.flink.graph.examples.data.SingleSourceShortestPathsData;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.graph.library.GSASingleSourceShortestPaths;
import org.apache.flink.graph.utils.GraphUtils.IdentityMapper;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/** Tests for gather-sum-apply. */
@RunWith(Parameterized.class)
public class GatherSumApplyITCase extends MultipleProgramsTestBase {

    public GatherSumApplyITCase(TestExecutionMode mode) {
        super(mode);
    }

    // --------------------------------------------------------------------------------------------
    //  Connected Components Test
    // --------------------------------------------------------------------------------------------

    private String expectedResultCC = "1,1\n" + "2,1\n" + "3,1\n" + "4,1\n";

    @Test
    public void testConnectedComponentsWithObjectReuseDisabled() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableObjectReuse();

        Graph<Long, Long, NullValue> inputGraph =
                Graph.fromDataSet(
                        ConnectedComponentsDefaultData.getDefaultEdgeDataSet(env),
                        new IdentityMapper<>(),
                        env);

        List<Vertex<Long, Long>> result =
                inputGraph.run(new GSAConnectedComponents<>(16)).collect();

        compareResultAsTuples(result, expectedResultCC);
    }

    @Test
    public void testConnectedComponentsWithObjectReuseEnabled() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        DataSet<Edge<LongValue, NullValue>> edges =
                Translate.translateEdgeIds(
                        ConnectedComponentsDefaultData.getDefaultEdgeDataSet(env),
                        new LongToLongValue());

        Graph<LongValue, LongValue, NullValue> inputGraph =
                Graph.fromDataSet(edges, new IdentityMapper<>(), env);

        List<Vertex<LongValue, LongValue>> result =
                inputGraph.run(new GSAConnectedComponents<>(16)).collect();

        compareResultAsTuples(result, expectedResultCC);
    }

    // --------------------------------------------------------------------------------------------
    //  Single Source Shortest Path Test
    // --------------------------------------------------------------------------------------------

    @Test
    public void testSingleSourceShortestPaths() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, NullValue, Double> inputGraph =
                Graph.fromDataSet(
                        SingleSourceShortestPathsData.getDefaultEdgeDataSet(env),
                        new InitMapperSSSP(),
                        env);

        List<Vertex<Long, Double>> result =
                inputGraph.run(new GSASingleSourceShortestPaths<>(1L, 16)).collect();

        String expectedResult = "1,0.0\n" + "2,12.0\n" + "3,13.0\n" + "4,47.0\n" + "5,48.0\n";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    private static final class InitMapperSSSP implements MapFunction<Long, NullValue> {
        public NullValue map(Long value) {
            return NullValue.getInstance();
        }
    }
}
