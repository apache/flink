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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.asm.dataset.Collect;
import org.apache.flink.graph.library.linkanalysis.HITS.Result;
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
 * Tests for {@link HITS}.
 */
public class HITSTest extends AsmTestBase {

	/*
	 * This test result can be verified with the following Python script.

	import math
	import networkx as nx

	graph=nx.read_edgelist('directedSimpleGraph.csv', delimiter=',', create_using=nx.DiGraph())
	hits=nx.algorithms.link_analysis.hits(graph, tol=0.000001)

	hubbiness_norm=math.sqrt(sum(v*v for v in hits[0].values()))
	authority_norm=math.sqrt(sum(v*v for v in hits[1].values()))

	for key in sorted(hits[0]):
		print('{}: {}, {}'.format(key, hits[0][key]/hubbiness_norm, hits[1][key]/authority_norm))
	 */
	@Test
	public void testWithSimpleGraph() throws Exception {
		DataSet<Result<IntValue>> hits = new HITS<IntValue, NullValue, NullValue>(20)
			.run(directedSimpleGraph);

		List<Tuple2<Double, Double>> expectedResults = new ArrayList<>();
		expectedResults.add(Tuple2.of(0.54464336064, 0.0));
		expectedResults.add(Tuple2.of(0.0, 0.836329364957));
		expectedResults.add(Tuple2.of(0.607227075863, 0.268492484699));
		expectedResults.add(Tuple2.of(0.54464336064, 0.395445020996));
		expectedResults.add(Tuple2.of(0.0, 0.268492484699));
		expectedResults.add(Tuple2.of(0.194942293412, 0.0));

		for (Result<IntValue> result : hits.collect()) {
			int id = result.getVertexId0().getValue();
			assertEquals(expectedResults.get(id).f0, result.getHubScore().getValue(), ACCURACY);
			assertEquals(expectedResults.get(id).f1, result.getAuthorityScore().getValue(), ACCURACY);
		}
	}

	/**
	 * Validate a test where each result has the same values.
	 *
	 * @param graph input graph
	 * @param count number of results
	 * @param score result hub and authority score
	 * @param <T> graph ID type
	 * @throws Exception on error
	 */
	private static <T> void validate(Graph<T, NullValue, NullValue> graph, long count, double score) throws Exception {
		DataSet<Result<T>> hits = new HITS<T, NullValue, NullValue>(ACCURACY)
			.run(graph);

		List<Result<T>> results = hits.collect();

		assertEquals(count, results.size());

		for (Result<T> result : results) {
			assertEquals(score, result.getHubScore().getValue(), ACCURACY);
			assertEquals(score, result.getAuthorityScore().getValue(), ACCURACY);
		}
	}

	@Test
	public void testWithCompleteGraph() throws Exception {
		validate(completeGraph, completeGraphVertexCount, 1.0 / Math.sqrt(completeGraphVertexCount));
	}

	@Test
	public void testWithEmptyGraphWithVertices() throws Exception {
		// the HITS implementation does not currently produce scores for
		// 0-degree vertices as this exclusion does not affect the scores of
		// other vertices in the graph
		validate(emptyGraphWithVertices, 0, Double.NaN);
	}

	@Test
	public void testWithEmptyGraphWithoutVertices() throws Exception {
		validate(emptyGraphWithoutVertices, 0, Double.NaN);
	}

	/*
	 * This test result can be verified with the following Python script.

	import math
	import networkx as nx

	graph=nx.read_edgelist('directedRMatGraph.csv', delimiter=',', create_using=nx.DiGraph())
	hits=nx.algorithms.link_analysis.hits(graph, tol=0.000001)

	hubbiness_norm=math.sqrt(sum(v*v for v in hits[0].values()))
	authority_norm=math.sqrt(sum(v*v for v in hits[1].values()))

	for key in [0, 1, 2, 8, 13, 29, 109, 394, 652, 1020]:
		print('{}: {}, {}'.format(key, hits[0][str(key)]/hubbiness_norm, hits[1][str(key)]/authority_norm))
	 */
	@Test
	public void testWithRMatGraph() throws Exception {
		DataSet<Result<LongValue>> hits = directedRMatGraph(10, 16)
			.run(new HITS<>(ACCURACY));

		Map<Long, Result<LongValue>> results = new HashMap<>();
		for (Result<LongValue> result :  new Collect<Result<LongValue>>().run(hits).execute()) {
			results.put(result.getVertexId0().getValue(), result);
		}

		assertEquals(902, results.size());

		Map<Long, Tuple2<Double, Double>> expectedResults = new HashMap<>();
		// a pseudo-random selection of results, both high and low
		expectedResults.put(0L, Tuple2.of(0.231077034503, 0.238110215657));
		expectedResults.put(1L, Tuple2.of(0.162364053853, 0.169679504542));
		expectedResults.put(2L, Tuple2.of(0.162412612418, 0.161015667467));
		expectedResults.put(8L, Tuple2.of(0.167064641648, 0.158592966732));
		expectedResults.put(13L, Tuple2.of(0.0419155956364, 0.0407091624972));
		expectedResults.put(29L, Tuple2.of(0.0102017346609, 0.0146218045619));
		expectedResults.put(109L, Tuple2.of(0.00190531000308, 0.00481944991974));
		expectedResults.put(394L, Tuple2.of(0.0122287016151, 0.0147987969383));
		expectedResults.put(652L, Tuple2.of(0.0109666592418, 0.0113713306828));
		expectedResults.put(1020L, Tuple2.of(0.0, 0.000326973733252));

		for (Map.Entry<Long, Tuple2<Double, Double>> expected : expectedResults.entrySet()) {
			double hubScore = results.get(expected.getKey()).getHubScore().getValue();
			double authorityScore = results.get(expected.getKey()).getAuthorityScore().getValue();

			assertEquals(expected.getValue().f0, hubScore, ACCURACY);
			assertEquals(expected.getValue().f1, authorityScore, ACCURACY);
		}
	}
}
