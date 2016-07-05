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

package org.apache.flink.graph.library.metric.directed;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.metric.directed.VertexMetrics.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VertexMetricsTest
extends AsmTestBase {

	@Test
	public void testWithSimpleGraph()
			throws Exception {
		Result expectedResult = new Result(6, 7, 13);

		Result vertexMetrics = new VertexMetrics<IntValue, NullValue, NullValue>()
			.run(undirectedSimpleGraph)
			.execute();

		assertEquals(expectedResult, vertexMetrics);
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;
		long expectedEdges = completeGraphVertexCount * expectedDegree / 2;
		long expectedTriplets = completeGraphVertexCount * CombinatoricsUtils.binomialCoefficient((int)expectedDegree, 2);

		Result expectedResult = new Result(completeGraphVertexCount, expectedEdges, expectedTriplets);

		Result vertexMetrics = new VertexMetrics<LongValue, NullValue, NullValue>()
			.run(completeGraph)
			.execute();

		assertEquals(expectedResult, vertexMetrics);
	}

	@Test
	public void testWithEmptyGraph()
			throws Exception {
		Result expectedResult;

		expectedResult = new Result(0, 0, 0);

		Result withoutZeroDegreeVertices = new VertexMetrics<LongValue, NullValue, NullValue>()
			.setIncludeZeroDegreeVertices(false)
			.run(emptyGraph)
			.execute();

		assertEquals(withoutZeroDegreeVertices, expectedResult);

		expectedResult = new Result(3, 0, 0);

		Result withZeroDegreeVertices = new VertexMetrics<LongValue, NullValue, NullValue>()
			.setIncludeZeroDegreeVertices(true)
			.run(emptyGraph)
			.execute();

		assertEquals(expectedResult, withZeroDegreeVertices);
	}

	@Test
	public void testWithRMatGraph()
			throws Exception {
		Result expectedResult = new Result(902, 10442, 1003442);

		Result withoutZeroDegreeVertices = new VertexMetrics<LongValue, NullValue, NullValue>()
			.run(undirectedRMatGraph)
			.execute();

		assertEquals(expectedResult, withoutZeroDegreeVertices);
	}
}
