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
public class HITSTest
extends AsmTestBase {

	/*
	 * This test result can be verified with the following Python script.

	import math
	import networkx as nx

	graph=nx.read_edgelist('directedSimpleGraph.csv', delimiter=',', create_using=nx.DiGraph())
	hits=nx.algorithms.link_analysis.hits(graph)

	hubbiness_norm=math.sqrt(sum(v*v for v in hits[0].values()))
	authority_norm=math.sqrt(sum(v*v for v in hits[1].values()))

	for key in sorted(hits[0]):
		print('{}: {}, {}'.format(key, hits[0][key]/hubbiness_norm, hits[1][key]/authority_norm))
	 */
	@Test
	public void testWithSimpleGraph()
			throws Exception {
		DataSet<Result<IntValue>> hits = new HITS<IntValue, NullValue, NullValue>(20)
			.run(directedSimpleGraph);

		List<Tuple2<Double, Double>> expectedResults = new ArrayList<>();
		expectedResults.add(Tuple2.of(0.544643396306, 0.0));
		expectedResults.add(Tuple2.of(0.0, 0.836329395866));
		expectedResults.add(Tuple2.of(0.607227031134, 0.268492526138));
		expectedResults.add(Tuple2.of(0.544643396306, 0.395444899355));
		expectedResults.add(Tuple2.of(0.0, 0.268492526138));
		expectedResults.add(Tuple2.of(0.194942233447, 0.0));

		for (Result<IntValue> result : hits.collect()) {
			int id = result.getVertexId0().getValue();
			assertEquals(expectedResults.get(id).f0, result.getHubScore().getValue(), 0.000001);
			assertEquals(expectedResults.get(id).f1, result.getAuthorityScore().getValue(), 0.000001);
		}
	}

	@Test
	public void testWithCompleteGraph()
			throws Exception {
		double expectedScore = 1.0 / Math.sqrt(completeGraphVertexCount);

		DataSet<Result<LongValue>> hits = new HITS<LongValue, NullValue, NullValue>(0.000001)
			.run(completeGraph);

		List<Result<LongValue>> results = hits.collect();

		assertEquals(completeGraphVertexCount, results.size());

		for (Result<LongValue> result : results) {
			assertEquals(expectedScore, result.getHubScore().getValue(), 0.000001);
			assertEquals(expectedScore, result.getAuthorityScore().getValue(), 0.000001);
		}
	}

	/*
	 * This test result can be verified with the following Python script.

	import math
	import networkx as nx

	graph=nx.read_edgelist('directedRMatGraph.csv', delimiter=',', create_using=nx.DiGraph())
	hits=nx.algorithms.link_analysis.hits(graph)

	hubbiness_norm=math.sqrt(sum(v*v for v in hits[0].values()))
	authority_norm=math.sqrt(sum(v*v for v in hits[1].values()))

	for key in [0, 1, 2, 8, 13, 29, 109, 394, 652, 1020]:
		print('{}: {}, {}'.format(key, hits[0][str(key)]/hubbiness_norm, hits[1][str(key)]/authority_norm))
	 */
	@Test
	public void testWithRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> hits = directedRMatGraph(10, 16)
			.run(new HITS<>(0.000001));

		Map<Long, Result<LongValue>> results = new HashMap<>();
		for (Result<LongValue> result :  new Collect<Result<LongValue>>().run(hits).execute()) {
			results.put(result.getVertexId0().getValue(), result);
		}

		assertEquals(902, results.size());

		Map<Long, Tuple2<Double, Double>> expectedResults = new HashMap<>();
		// a pseudo-random selection of results, both high and low
		expectedResults.put(0L, Tuple2.of(0.231077034747, 0.238110214937));
		expectedResults.put(1L, Tuple2.of(0.162364053933, 0.169679504287));
		expectedResults.put(2L, Tuple2.of(0.162412612499, 0.161015667261));
		expectedResults.put(8L, Tuple2.of(0.167064641724, 0.158592966505));
		expectedResults.put(13L, Tuple2.of(0.041915595624, 0.0407091625629));
		expectedResults.put(29L, Tuple2.of(0.0102017346511, 0.0146218045999));
		expectedResults.put(109L, Tuple2.of(0.00190531000389, 0.00481944993023));
		expectedResults.put(394L, Tuple2.of(0.0122287016161, 0.0147987969538));
		expectedResults.put(652L, Tuple2.of(0.010966659242, 0.0113713306749));
		expectedResults.put(1020L, Tuple2.of(0.0, 0.000326973732127));

		for (Map.Entry<Long, Tuple2<Double, Double>> expected : expectedResults.entrySet()) {
			double hubScore = results.get(expected.getKey()).getHubScore().getValue();
			double authorityScore = results.get(expected.getKey()).getAuthorityScore().getValue();

			assertEquals(expected.getValue().f0, hubScore, 0.00001);
			assertEquals(expected.getValue().f1, authorityScore, 0.00001);
		}
	}
}
