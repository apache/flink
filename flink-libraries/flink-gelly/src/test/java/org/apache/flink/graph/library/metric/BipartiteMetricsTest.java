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

package org.apache.flink.graph.library.metric;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.bipartite.BipartiteEdge;
import org.apache.flink.graph.bipartite.BipartiteGraph;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BipartiteMetricsTest {

	private BipartiteMetrics bipartiteMetrics = new BipartiteMetrics();
	private BipartiteMetrics.Result result;

	@Before
	public void before() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		result = bipartiteMetrics
			.run(bipartiteGraph)
			.execute();
	}

	@Test
	public void testNumberOfTopVertices() {
		assertEquals(3, result.getNumberOfTopVertices());
	}

	@Test
	public void testNumberOfBottomVertices() {
		assertEquals(4, result.getNumberOfBottomVertices());
	}

	@Test
	public void testMaximumTopDegree() {
		assertEquals(4, result.getMaximumTopDegree());
	}

	@Test
	public void testMaximumBottomDegree() {
		assertEquals(3, result.getMaximumBottomDegree());
	}

	@Test
	public void testGetNumberOfEdges() {
		assertEquals(7, result.getNumberOfEdges());
	}

	private BipartiteGraph<Integer, Integer, String, String, String> createBipartiteGraph() {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();

		DataSet<Vertex<Integer, String>> bottomVertices = executionEnvironment.fromCollection(Arrays.asList(
			new Vertex<>(1, "bottom1"),
			new Vertex<>(2, "bottom2"),
			new Vertex<>(3, "bottom3"),
			new Vertex<>(4, "bottom4")
		));

		DataSet<Vertex<Integer, String>> topVertices = executionEnvironment.fromCollection(Arrays.asList(
			new Vertex<>(4, "top4"),
			new Vertex<>(5, "top5"),
			new Vertex<>(6, "top6")
		));

		DataSet<BipartiteEdge<Integer, Integer, String>> edges = executionEnvironment.fromCollection(Arrays.asList(
			new BipartiteEdge<>(4, 1, "4-1"),
			new BipartiteEdge<>(5, 1, "5-1"),
			new BipartiteEdge<>(5, 2, "5-2"),
			new BipartiteEdge<>(6, 2, "6-2"),
			new BipartiteEdge<>(6, 3, "6-3"),
			new BipartiteEdge<>(6, 1, "6-1"),
			new BipartiteEdge<>(6, 4, "6-4")
		));

		return BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, executionEnvironment);
	}
}
