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

package org.apache.flink.graph.library.clustering.directed;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.NullValue;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GlobalClusteringCoefficient}.
 */
public class GlobalClusteringCoefficientTest extends AsmTestBase {

	/**
	 * Validate a test result.
	 *
	 * @param graph input graph
	 * @param tripletCount result triplet count
	 * @param triangleCount result triangle count
	 * @param <T> graph ID type
	 * @throws Exception on error
	 */
	private static <T extends Comparable<T> & CopyableValue<T>> void validate(
			Graph<T, NullValue, NullValue> graph, long tripletCount, long triangleCount) throws Exception {
		Result result = new GlobalClusteringCoefficient<T, NullValue, NullValue>()
			.run(graph)
			.execute();

		assertEquals(tripletCount, result.getNumberOfTriplets());
		assertEquals(triangleCount, result.getNumberOfTriangles());
	}

	@Test
	public void testWithSimpleGraph() throws Exception {
		validate(directedSimpleGraph, 13, 6);
	}

	@Test
	public void testWithCompleteGraph() throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;
		long expectedCount = completeGraphVertexCount * CombinatoricsUtils.binomialCoefficient((int) expectedDegree, 2);

		validate(completeGraph, expectedCount, expectedCount);
	}

	@Test
	public void testWithEmptyGraphWithVertices() throws Exception {
		validate(emptyGraphWithVertices, 0, 0);
	}

	@Test
	public void testWithEmptyGraphWithoutVertices() throws Exception {
		validate(emptyGraphWithoutVertices, 0, 0);
	}

	@Test
	public void testWithRMatGraph() throws Exception {
		validate(directedRMatGraph(10, 16), 1003442, 225147);
	}
}
