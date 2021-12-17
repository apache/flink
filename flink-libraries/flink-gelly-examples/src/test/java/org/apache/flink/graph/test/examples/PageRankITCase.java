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

package org.apache.flink.graph.test.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.GSAPageRank;
import org.apache.flink.graph.examples.PageRank;
import org.apache.flink.graph.examples.data.PageRankData;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/** Tests for {@link PageRank}. */
@RunWith(Parameterized.class)
public class PageRankITCase extends MultipleProgramsTestBase {

    public PageRankITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testPageRankWithThreeIterations() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Double, Double> inputGraph =
                Graph.fromDataSet(PageRankData.getDefaultEdgeDataSet(env), new InitMapper(), env);

        List<Vertex<Long, Double>> result = inputGraph.run(new PageRank<>(0.85, 3)).collect();

        compareWithDelta(result, 0.01);
    }

    @Test
    public void testGSAPageRankWithThreeIterations() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Double, Double> inputGraph =
                Graph.fromDataSet(PageRankData.getDefaultEdgeDataSet(env), new InitMapper(), env);

        List<Vertex<Long, Double>> result = inputGraph.run(new GSAPageRank<>(0.85, 3)).collect();

        compareWithDelta(result, 0.01);
    }

    @Test
    public void testPageRankWithThreeIterationsAndNumOfVertices() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Double, Double> inputGraph =
                Graph.fromDataSet(PageRankData.getDefaultEdgeDataSet(env), new InitMapper(), env);

        List<Vertex<Long, Double>> result = inputGraph.run(new PageRank<>(0.85, 3)).collect();

        compareWithDelta(result, 0.01);
    }

    @Test
    public void testGSAPageRankWithThreeIterationsAndNumOfVertices() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Double, Double> inputGraph =
                Graph.fromDataSet(PageRankData.getDefaultEdgeDataSet(env), new InitMapper(), env);

        List<Vertex<Long, Double>> result = inputGraph.run(new GSAPageRank<>(0.85, 3)).collect();

        compareWithDelta(result, 0.01);
    }

    private void compareWithDelta(List<Vertex<Long, Double>> result, double delta) {

        String resultString = "";
        for (Vertex<Long, Double> v : result) {
            resultString += v.f0.toString() + "," + v.f1.toString() + "\n";
        }

        String expectedResult = PageRankData.RANKS_AFTER_3_ITERATIONS;
        String[] expected = expectedResult.isEmpty() ? new String[0] : expectedResult.split("\n");

        String[] resultArray = resultString.isEmpty() ? new String[0] : resultString.split("\n");

        Arrays.sort(expected);
        Arrays.sort(resultArray);

        for (int i = 0; i < expected.length; i++) {
            String[] expectedFields = expected[i].split(",");
            String[] resultFields = resultArray[i].split(",");

            double expectedPayLoad = Double.parseDouble(expectedFields[1]);
            double resultPayLoad = Double.parseDouble(resultFields[1]);

            Assert.assertTrue(
                    "Values differ by more than the permissible delta",
                    Math.abs(expectedPayLoad - resultPayLoad) < delta);
        }
    }

    @SuppressWarnings("serial")
    private static final class InitMapper implements MapFunction<Long, Double> {
        public Double map(Long value) {
            return 1.0;
        }
    }
}
