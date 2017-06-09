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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Tests for {@link Graph#inDegrees()}, {@link Graph#outDegrees()},
 * and {@link Graph#getDegrees()}.
 */
@RunWith(Parameterized.class)
public class DegreesITCase extends MultipleProgramsTestBase {

	public DegreesITCase(TestExecutionMode mode) {
		super(mode);
	}

	private String expectedResult;

	@Test
	public void testOutDegrees() throws Exception {
		/*
		* Test outDegrees()
		*/
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, LongValue>> data = graph.outDegrees();
		List<Tuple2<Long, LongValue>> result = data.collect();

		expectedResult = "1,2\n" +
			"2,1\n" +
			"3,2\n" +
			"4,1\n" +
			"5,1\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testOutDegreesWithNoOutEdges() throws Exception {
		/*
		 * Test outDegrees() no outgoing edges
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeDataWithZeroDegree(env), env);

		DataSet<Tuple2<Long, LongValue>> data = graph.outDegrees();
		List<Tuple2<Long, LongValue>> result = data.collect();

		expectedResult = "1,3\n" +
			"2,1\n" +
			"3,1\n" +
			"4,1\n" +
			"5,0\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testInDegrees() throws Exception {
		/*
		 * Test inDegrees()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, LongValue>> data = graph.inDegrees();
		List<Tuple2<Long, LongValue>> result = data.collect();

		expectedResult = "1,1\n" +
			"2,1\n" +
			"3,2\n" +
			"4,1\n" +
			"5,2\n";
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testInDegreesWithNoInEdge() throws Exception {
		/*
		 * Test inDegrees() no ingoing edge
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeDataWithZeroDegree(env), env);

		DataSet<Tuple2<Long, LongValue>> data = graph.inDegrees();
		List<Tuple2<Long, LongValue>> result = data.collect();

		expectedResult = "1,0\n" +
			"2,1\n" +
			"3,1\n" +
			"4,1\n" +
			"5,3\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testGetDegrees() throws Exception {
		/*
		 * Test getDegrees()
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
			TestGraphUtils.getLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, LongValue>> data = graph.getDegrees();
		List<Tuple2<Long, LongValue>> result = data.collect();

		expectedResult = "1,3\n" +
			"2,2\n" +
			"3,4\n" +
			"4,2\n" +
			"5,3\n";

		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testGetDegreesWithDisconnectedData() throws Exception {
		/*
		 * Test getDegrees() with disconnected data
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, NullValue, Long> graph =
			Graph.fromDataSet(TestGraphUtils.getDisconnectedLongLongEdgeData(env), env);

		DataSet<Tuple2<Long, LongValue>> data = graph.outDegrees();
		List<Tuple2<Long, LongValue>> result = data.collect();

		expectedResult = "1,2\n" +
			"2,1\n" +
			"3,0\n" +
			"4,1\n" +
			"5,0\n";

		compareResultAsTuples(result, expectedResult);
	}
}
