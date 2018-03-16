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

package org.apache.flink.graph.library.clustering.undirected;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;
import org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient.Result;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LocalClusteringCoefficient}.
 */
public class LocalClusteringCoefficientTest extends AsmTestBase {

	@Test
	public void testSimpleGraph() throws Exception {
		String expectedResult =
			"(0,2,1)\n" +
			"(1,3,2)\n" +
			"(2,3,2)\n" +
			"(3,4,1)\n" +
			"(4,1,0)\n" +
			"(5,1,0)";

		DataSet<Result<IntValue>> cc = undirectedSimpleGraph
			.run(new LocalClusteringCoefficient<>());

		TestBaseUtils.compareResultAsText(cc.collect(), expectedResult);
	}

	/**
	 * Validate a test where each result has the same values.
	 *
	 * @param graph input graph
	 * @param count number of results
	 * @param degree result degree
	 * @param triangleCount result triangle count
	 * @param <T> graph ID type
	 * @throws Exception on error
	 */
	private static <T extends Comparable<T> & CopyableValue<T>> void validate(
			Graph<T, NullValue, NullValue> graph, long count, long degree, long triangleCount) throws Exception {
		DataSet<Result<T>> cc = graph
			.run(new LocalClusteringCoefficient<>());

		List<Result<T>> results = cc.collect();

		assertEquals(count, results.size());

		for (Result<T> result : results) {
			assertEquals(degree, result.getDegree().getValue());
			assertEquals(triangleCount, result.getTriangleCount().getValue());
		}
	}

	@Test
	public void testCompleteGraph() throws Exception {
		long degree = completeGraphVertexCount - 1;
		long triangleCount = CombinatoricsUtils.binomialCoefficient((int) degree, 2);

		validate(completeGraph, completeGraphVertexCount, degree, triangleCount);
	}

	@Test
	public void testWithEmptyGraphWithVertices() throws Exception {
		validate(emptyGraphWithVertices, emptyGraphVertexCount, 0, 0);
	}

	@Test
	public void testWithEmptyGraphWithoutVertices() throws Exception {
		validate(emptyGraphWithoutVertices, 0, 0, 0);
	}

	@Test
	public void testRMatGraph() throws Exception {
		DataSet<Result<LongValue>> cc = undirectedRMatGraph(10, 16)
			.run(new LocalClusteringCoefficient<>());

		Checksum checksum = new ChecksumHashCode<Result<LongValue>>()
			.run(cc)
			.execute();

		assertEquals(902, checksum.getCount());
		assertEquals(0x000001cab2d3677bL, checksum.getChecksum());
	}
}
