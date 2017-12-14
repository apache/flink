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
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.data.LabelPropagationData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Tests for {@link LabelPropagation}.
 */
@RunWith(Parameterized.class)
public class LabelPropagationITCase extends MultipleProgramsTestBase {

	public LabelPropagationITCase(TestExecutionMode mode) {
		super(mode);
	}

	private String expectedResult;

	@Test
	public void testSingleIteration() throws Exception {
		/*
		 * Test one iteration of label propagation example with a simple graph
		 */
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, NullValue> inputGraph = Graph.fromDataSet(
			LabelPropagationData.getDefaultVertexSet(env),
			LabelPropagationData.getDefaultEdgeDataSet(env), env);

		List<Vertex<Long, Long>> result = inputGraph
			.run(new LabelPropagation<>(1))
			.collect();

		expectedResult = LabelPropagationData.LABELS_AFTER_1_ITERATION;
		compareResultAsTuples(result, expectedResult);
	}

	@Test
	public void testTieBreaker() throws Exception {
		/*
		 * Test the label propagation example where a tie must be broken
		 */
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Graph<Long, Long, NullValue> inputGraph = Graph.fromDataSet(
			LabelPropagationData.getTieVertexSet(env),
			LabelPropagationData.getTieEdgeDataSet(env), env);

		List<Vertex<Long, Long>> result = inputGraph
			.run(new LabelPropagation<>(1))
			.collect();

		expectedResult = LabelPropagationData.LABELS_WITH_TIE;
		compareResultAsTuples(result, expectedResult);
	}
}
