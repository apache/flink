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

package org.apache.flink.graph.bipartite;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.LongValue;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.graph.generator.TestUtils.compareGraph;
import static org.junit.Assert.assertEquals;

public class BipartiteGraphTest {

	@Test
	public void testGetTopVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		TestBaseUtils.compareResultAsText(bipartiteGraph.getTopVertices().collect(),
			"(4,top4)\n" +
			"(5,top5)\n" +
			"(6,top6)");
	}

	@Test
	public void testGetBottomVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		TestBaseUtils.compareResultAsText(bipartiteGraph.getBottomVertices().collect(),
			"(1,bottom1)\n" +
			"(2,bottom2)\n" +
			"(3,bottom3)");
	}

	@Test
	public void testGetEdges() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		TestBaseUtils.compareResultAsText(bipartiteGraph.getEdges().collect(),
			"(4,1,4-1)\n" +
			"(5,1,5-1)\n" +
			"(5,2,5-2)\n" +
			"(6,2,6-2)\n" +
			"(6,3,6-3)");
	}

	@Test
	public void testGetTopVerticesIds() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		DataSet<Integer> topVerticesIds = bipartiteGraph.getTopVertexIds();

		TestBaseUtils.compareResultAsText(topVerticesIds.collect(),
			"4\n5\n6");
	}

	@Test
	public void testGetBottomVerticesIds() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		DataSet<Integer> bottomVerticesIds = bipartiteGraph.getBottomVertexIds();

		TestBaseUtils.compareResultAsText(bottomVerticesIds.collect(),
			"1\n2\n3");
	}

	@Test
	public void testEdgeIds() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		DataSet<Tuple2<Integer, Integer>> edgeIds = bipartiteGraph.getEdgeIds();

		TestBaseUtils.compareResultAsText(edgeIds.collect(),
			"(4,1)\n" +
			"(5,1)\n" +
			"(5,2)\n" +
			"(6,2)\n" +
			"(6,3)");
	}

	@Test
	public void testNumberOfTopVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		assertEquals(3, bipartiteGraph.numberOfTopVertices());
	}

	@Test
	public void testNumberOfBottomVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		assertEquals(3, bipartiteGraph.numberOfBottomVertices());
	}

	@Test
	public void testNumberOfEdges() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		assertEquals(5, bipartiteGraph.numberOfEdges());
	}

	@Test
	public void testTopDegrees() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		DataSet<Tuple2<Integer, LongValue>> topDegrees = bipartiteGraph.topDegrees();

		TestBaseUtils.compareResultAsText(topDegrees.collect(),
			"(4,1)\n" +
			"(5,2)\n" +
			"(6,2)");
	}

	@Test
	public void testBottomDegrees() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		DataSet<Tuple2<Integer, LongValue>> bottomDegrees = bipartiteGraph.bottomDegrees();

		TestBaseUtils.compareResultAsText(bottomDegrees.collect(),
			"(1,2)\n" +
			"(2,2)\n" +
			"(3,1)");
	}

	@Test
	public void testGetTuples() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		DataSet<Tuple5<Integer, Integer, String, String, String>> tuples = bipartiteGraph.getTuples();

		TestBaseUtils.compareResultAsText(tuples.collect(),
			"(4,1,top4,bottom1,4-1)\n" +
			"(5,1,top5,bottom1,5-1)\n" +
			"(5,2,top5,bottom2,5-2)\n" +
			"(6,2,top6,bottom2,6-2)\n" +
			"(6,3,top6,bottom3,6-3)");
	}

	@Test
	public void testSimpleTopProjection() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();
		Graph<Integer, String, Tuple2<String, String>> graph = bipartiteGraph.projectionTopSimple();

		compareGraph(graph, "4; 5; 6", "5,4; 4,5; 5,6; 6,5");

		String expected =
			"(5,4,(5-1,4-1))\n" +
			"(4,5,(4-1,5-1))\n" +
			"(6,5,(6-2,5-2))\n" +
			"(5,6,(5-2,6-2))";
		TestBaseUtils.compareResultAsText(graph.getEdges().collect(), expected);
	}

	@Test
	public void testSimpleBottomProjection() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();
		Graph<Integer, String, Tuple2<String, String>> graph = bipartiteGraph.projectionBottomSimple();

		compareGraph(graph, "1; 2; 3", "1,2; 2,1; 2,3; 3,2");

		String expected =
			"(3,2,(6-3,6-2))\n" +
			"(2,3,(6-2,6-3))\n" +
			"(2,1,(5-2,5-1))\n" +
			"(1,2,(5-1,5-2))";
		TestBaseUtils.compareResultAsText(graph.getEdges().collect(), expected);
	}

	@Test
	public void testFullTopProjection() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();
		Graph<Integer, String, Projection<Integer, String, String, String>> graph = bipartiteGraph.projectionTopFull();

		compareGraph(graph, "4; 5; 6", "5,4; 4,5; 5,6; 6,5");

		String expected =
			"(5,4,(1,bottom1,top5,top4,5-1,4-1))\n" +
			"(4,5,(1,bottom1,top4,top5,4-1,5-1))\n" +
			"(6,5,(2,bottom2,top6,top5,6-2,5-2))\n" +
			"(5,6,(2,bottom2,top5,top6,5-2,6-2))";
		TestBaseUtils.compareResultAsText(graph.getEdges().collect(), expected);
	}

	@Test
	public void testFullBottomProjection() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();
		Graph<Integer, String, Projection<Integer, String, String, String>> graph = bipartiteGraph.projectionBottomFull();

		compareGraph(graph, "1; 2; 3", "1,2; 2,1; 2,3; 3,2");

		String expected =
			"(3,2,(6,top6,bottom3,bottom2,6-3,6-2))\n" +
			"(2,3,(6,top6,bottom2,bottom3,6-2,6-3))\n" +
			"(2,1,(5,top5,bottom2,bottom1,5-2,5-1))\n" +
			"(1,2,(5,top5,bottom1,bottom2,5-1,5-2))";
		TestBaseUtils.compareResultAsText(graph.getEdges().collect(), expected);
	}

	private BipartiteGraph<Integer, Integer, String, String, String> createBipartiteGraph() {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();

		DataSet<Vertex<Integer, String>> topVertices = executionEnvironment.fromCollection(Arrays.asList(
			new Vertex<>(4, "top4"),
			new Vertex<>(5, "top5"),
			new Vertex<>(6, "top6")
		));

		DataSet<Vertex<Integer, String>> bottomVertices = executionEnvironment.fromCollection(Arrays.asList(
			new Vertex<>(1, "bottom1"),
			new Vertex<>(2, "bottom2"),
			new Vertex<>(3, "bottom3")
		));

		DataSet<BipartiteEdge<Integer, Integer, String>> edges = executionEnvironment.fromCollection(Arrays.asList(
			new BipartiteEdge<>(4, 1, "4-1"),
			new BipartiteEdge<>(5, 1, "5-1"),
			new BipartiteEdge<>(5, 2, "5-2"),
			new BipartiteEdge<>(6, 2, "6-2"),
			new BipartiteEdge<>(6, 3, "6-3")
		));

		return BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, executionEnvironment);
	}
}
