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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.ConnectedComponentsDefaultData;
import org.apache.flink.graph.example.utils.SingleSourceShortestPathsData;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.graph.library.GSASingleSourceShortestPaths;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(Parameterized.class)
public class GatherSumApplyITCase extends MultipleProgramsTestBase {

	public GatherSumApplyITCase(TestExecutionMode mode){
		super(mode);
	}

	private String expectedResult;

	// --------------------------------------------------------------------------------------------
	//  Connected Components Test
	// --------------------------------------------------------------------------------------------

	@Test
	public void testConnectedComponents() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, NullValue> inputGraph = Graph.fromDataSet(
				ConnectedComponentsDefaultData.getDefaultEdgeDataSet(env),
				new InitMapperCC(), env);

        List<Vertex<Long, Long>> result = inputGraph.run(
        		new GSAConnectedComponents<Long, NullValue>(16)).collect();

		expectedResult = "1,1\n" +
				"2,1\n" +
				"3,1\n" +
				"4,1\n";

		compareResultAsTuples(result, expectedResult);
	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path Test
	// --------------------------------------------------------------------------------------------

	@Test
	public void testSingleSourceShortestPaths() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Double, Double> inputGraph = Graph.fromDataSet(
				SingleSourceShortestPathsData.getDefaultEdgeDataSet(env),
				new InitMapperSSSP(), env);

        List<Vertex<Long, Double>> result = inputGraph.run(
        		new GSASingleSourceShortestPaths<Long>(1l, 16)).collect();

		expectedResult = "1,0.0\n" +
				"2,12.0\n" +
				"3,13.0\n" +
				"4,47.0\n" +
				"5,48.0\n";

		compareResultAsTuples(result, expectedResult);
	}

	@SuppressWarnings("serial")
	private static final class InitMapperCC implements MapFunction<Long, Long> {
		public Long map(Long value) {
			return value;
		}
	}

	@SuppressWarnings("serial")
	private static final class InitMapperSSSP implements MapFunction<Long, Double> {
		public Double map(Long value) {
			return 0.0;
		}
	}
}