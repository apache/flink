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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.graph.generator.TestUtils.compareGraph;
import static org.junit.Assert.assertEquals;

public class BipartiteGraphTest {
	private ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();

	@Test
	public void testCreateGraphFromVerticesAndEdgeCollections() throws Exception {
		List<Vertex<Integer, String>> topVertices = Arrays.asList(
			new Vertex<>(1, "top1"),
			new Vertex<>(3, "top3")
		);

		List<Vertex<Integer, String>> bottomVertices = Arrays.asList(
			new Vertex<>(2, "bottom2"),
			new Vertex<>(4, "bottom4")
		);

		List<BipartiteEdge<Integer, Integer, String>> edges = Arrays.asList(
			new BipartiteEdge<>(1, 2, "1-2"),
			new BipartiteEdge<>(3, 4, "3-4")
		);

		BipartiteGraph<Integer, Integer, String, String, String> graph
			= BipartiteGraph.fromCollection(topVertices, bottomVertices, edges, executionEnvironment);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,top1)\n" +
			"(3,top3)");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(2,bottom2)\n" +
			"(4,bottom4)");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,2,1-2)\n" +
			"(3,4,3-4)");
	}

	@Test
	public void testCreateGraphFromEdgeCollection() throws Exception {
		List<BipartiteEdge<Integer, Integer, String>> edges = Arrays.asList(
			new BipartiteEdge<>(1, 2, "1-2"),
			new BipartiteEdge<>(3, 4, "3-4")
		);

		BipartiteGraph<Integer, Integer, NullValue, NullValue, String> graph
			= BipartiteGraph.fromCollection(edges, executionEnvironment);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,(null))\n" +
			"(3,(null))");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(2,(null))\n" +
			"(4,(null))");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,2,1-2)\n" +
			"(3,4,3-4)");
	}

	@Test
	public void testCreateGraphFromEdgeCollectionAndInitializers() throws Exception {
		List<BipartiteEdge<Integer, Integer, String>> edges = Arrays.asList(
			new BipartiteEdge<>(1, 2, "1-2"),
			new BipartiteEdge<>(3, 4, "3-4")
		);

		BipartiteGraph<Integer, Integer, String, String, String> graph
			= BipartiteGraph.fromCollection(
				edges,
				new TopVertexInitializer(),
				new BottomVertexInitializer(),
				executionEnvironment);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,top1)\n" +
			"(3,top3)");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(2,bottom2)\n" +
			"(4,bottom4)");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,2,1-2)\n" +
			"(3,4,3-4)");
	}

	@Test
	public void testCreateGraphFromTuple2DataSet() throws Exception {
		DataSet<Tuple2<Integer, Integer>> tuple2DataSet = executionEnvironment.fromCollection(Arrays.asList(
			new Tuple2<>(1, 2),
			new Tuple2<>(3, 2),
			new Tuple2<>(5, 4)
		));

		BipartiteGraph<Integer, Integer, NullValue, NullValue, NullValue> graph = BipartiteGraph.fromTuple2DataSet(tuple2DataSet, executionEnvironment);

		TestBaseUtils.compareResultAsText(graph.getTopVertices().collect(),
			"(1,(null))\n" +
			"(3,(null))\n" +
			"(5,(null))");

		TestBaseUtils.compareResultAsText(graph.getBottomVertices().collect(),
			"(2,(null))\n" +
			"(4,(null))");

		TestBaseUtils.compareResultAsText(graph.getEdges().collect(),
			"(1,2,(null))\n" +
			"(3,2,(null))\n" +
			"(5,4,(null))");
	}

	@Test
	public void testGetTopVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		assertEquals(
			Arrays.asList(
				new Vertex<>(4, "top4"),
				new Vertex<>(5, "top5"),
				new Vertex<>(6, "top6")),
			bipartiteGraph.getTopVertices().collect());
	}

	@Test
	public void testGetBottomVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		assertEquals(
			Arrays.asList(
				new Vertex<>(1, "bottom1"),
				new Vertex<>(2, "bottom2"),
				new Vertex<>(3, "bottom3")),
			bipartiteGraph.getBottomVertices().collect());
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

		graph.getEdges().print();
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

	private static class TopVertexInitializer implements MapFunction<Integer, String> {
		@Override
		public String map(Integer value) throws Exception {
			return "top" + value;
		}
	}

	private static class BottomVertexInitializer implements MapFunction<Integer, String> {
		@Override
		public String map(Integer value) throws Exception {
			return "bottom" + value;
		}
	}
}
