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

package org.apache.flink.graph.library.linkanalysis;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.library.linkanalysis.PageRank.Result;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PageRank}.
 */
public class PageRankTest
extends AsmTestBase {

	private static final double DAMPING_FACTOR = 0.85;

	/*
	 * This test result can be verified with the following Python script.

	import networkx as nx

	graph=nx.read_edgelist('directedSimpleGraph.csv', delimiter=',', create_using=nx.DiGraph())
	pagerank=nx.algorithms.link_analysis.pagerank(graph)

	for key in sorted(pagerank):
		print('{}: {}'.format(key, pagerank[key]))
	 */
	@Test
	public void testWithSimpleGraph()
			throws Exception {
		DataSet<Result<IntValue>> pr = new PageRank<IntValue, NullValue, NullValue>(DAMPING_FACTOR, 10)
			.run(directedSimpleGraph);

		List<Double> expectedResults = new ArrayList<>();
		expectedResults.add(0.09091296131286301);
		expectedResults.add(0.27951855944178117);
		expectedResults.add(0.12956847924535586);
		expectedResults.add(0.22329643739217675);
		expectedResults.add(0.18579060129496028);
		expectedResults.add(0.09091296131286301);

		for (Result<IntValue> result : pr.collect()) {
			int id = result.getVertexId0().getValue();
			assertEquals(expectedResults.get(id), result.getPageRankScore().getValue(), 0.000001);
		}
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		double expectedScore = 1.0 / completeGraphVertexCount;

		DataSet<Result<LongValue>> pr = new PageRank<LongValue, NullValue, NullValue>(DAMPING_FACTOR, 0.000001)
			.run(completeGraph);

		List<Result<LongValue>> results = pr.collect();

		assertEquals(completeGraphVertexCount, results.size());

		for (Result<LongValue> result : results) {
			assertEquals(expectedScore, result.getPageRankScore().getValue(), 0.000001);
		}
	}

	/*
	 * This test result can be verified with the following Python script.

	import networkx as nx

	graph=nx.read_edgelist('directedRMatGraph.csv', delimiter=',', create_using=nx.DiGraph())
	pagerank=nx.algorithms.link_analysis.pagerank(graph)

	for key in [0, 1, 2, 8, 13, 29, 109, 394, 652, 1020]:
		print('{}: {}'.format(key, pagerank[str(key)]))
	 */
	@Test
	public void testWithRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> pr = new PageRank<LongValue, NullValue, NullValue>(DAMPING_FACTOR, 0.000001)
			.run(directedRMatGraph(10, 16));

		Map<Long, Result<LongValue>> results = new HashMap<>();
		for (Result<LongValue> result :  new Collect<Result<LongValue>>().run(pr).execute()) {
			results.put(result.getVertexId0().getValue(), result);
		}

		assertEquals(902, results.size());

		Map<Long, Double> expectedResults = new HashMap<>();
		// a pseudo-random selection of results, both high and low
		expectedResults.put(0L, 0.027111807822);
		expectedResults.put(1L, 0.0132842310382);
		expectedResults.put(2L, 0.0121818392504);
		expectedResults.put(8L, 0.0115916809743);
		expectedResults.put(13L, 0.00183249490033);
		expectedResults.put(29L, 0.000848095047082);
		expectedResults.put(109L, 0.000308507844048);
		expectedResults.put(394L, 0.000828743280246);
		expectedResults.put(652L, 0.000684102931253);
		expectedResults.put(1020L, 0.000250487135148);

		for (Map.Entry<Long, Double> expected : expectedResults.entrySet()) {
			double value = results.get(expected.getKey()).getPageRankScore().getValue();

			assertEquals(expected.getValue(), value, 0.00001);
		}
	}
}
