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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.graph.generator.TestUtils.compareGraph;
import static org.junit.Assert.assertEquals;

public class BipartiteGraphTest {

	private ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();

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
	public void testMapTopVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.mapTopVertices(new MapVertex());

		TestBaseUtils.compareResultAsText(bipartiteGraph.getTopVertices().collect(),
			"(4,top4_mapped)\n" +
			"(5,top5_mapped)\n" +
			"(6,top6_mapped)");
	}

	@Test
	public void testMapBottomVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.mapBottomVertices(new MapVertex());

		TestBaseUtils.compareResultAsText(bipartiteGraph.getBottomVertices().collect(),
			"(1,bottom1_mapped)\n" +
			"(2,bottom2_mapped)\n" +
			"(3,bottom3_mapped)");
	}

	@Test
	public void testMapEdges() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.mapEdges(new MapEdge());

		TestBaseUtils.compareResultAsText(bipartiteGraph.getEdges().collect(),
			"(4,1,4-1_mapped)\n" +
			"(5,1,5-1_mapped)\n" +
			"(5,2,5-2_mapped)\n" +
			"(6,2,6-2_mapped)\n" +
			"(6,3,6-3_mapped)");
	}

	@Test
	public void testJoinWithTopVertices() throws Exception {
		DataSet<Tuple2<Integer, String>> toJoin = executionEnvironment.fromCollection(Arrays.asList(
			new Tuple2<>(4, "_val4"),
			new Tuple2<>(5, "_val5")
		));


		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.joinWithTopVertices(toJoin, new AppendVertexFunction());

		TestBaseUtils.compareResultAsText(bipartiteGraph.getTopVertices().collect(),
			"(4,top4_val4)\n" +
			"(5,top5_val5)\n" +
			"(6,top6)");
	}

	@Test
	public void testJoinWithBottomVertices() throws Exception {
		DataSet<Tuple2<Integer, String>> toJoin = executionEnvironment.fromCollection(Arrays.asList(
			new Tuple2<>(1, "_val1"),
			new Tuple2<>(2, "_val2")
		));


		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.joinWithBottomVertices(toJoin, new AppendVertexFunction());

		TestBaseUtils.compareResultAsText(bipartiteGraph.getBottomVertices().collect(),
			"(1,bottom1_val1)\n" +
			"(2,bottom2_val2)\n" +
			"(3,bottom3)");
	}

	@Test
	public void testJoinWithEdges() throws Exception {
		DataSet<Tuple3<Integer, Integer, String>> toJoin = executionEnvironment.fromCollection(Arrays.asList(
			new Tuple3<>(5, 1, "_val5-1"),
			new Tuple3<>(5, 2, "_val5-2"),
			new Tuple3<>(6, 2, "_val6-2")
		));

		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.joinWithEdges(toJoin, new AppendEdgeFunction());

		TestBaseUtils.compareResultAsText(bipartiteGraph.getEdges().collect(),
			"(4,1,4-1)\n" +
			"(5,1,5-1_val5-1)\n" +
			"(5,2,5-2_val5-2)\n" +
			"(6,2,6-2_val6-2)\n" +
			"(6,3,6-3)");
	}

	@Test
	public void testFilterOnTopVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.filterOnTopVertices(new FilterFunction<Vertex<Integer, String>>() {
				@Override
				public boolean filter(Vertex<Integer, String> value) throws Exception {
					return !value.getId().equals(4);
				}
			});

		TestBaseUtils.compareResultAsText(bipartiteGraph.getTopVertices().collect(),
			"(5,top5)\n" +
			"(6,top6)");

		TestBaseUtils.compareResultAsText(bipartiteGraph.getEdges().collect(),
			"(5,1,5-1)\n" +
			"(5,2,5-2)\n" +
			"(6,2,6-2)\n" +
			"(6,3,6-3)");
	}

	@Test
	public void testSubgraph() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.subgraph(
				new FilterFunction<Vertex<Integer, String>>() {
					@Override
					public boolean filter(Vertex<Integer, String> value) throws Exception {
						return !value.getId().equals(4);
					}
				},
				new FilterFunction<Vertex<Integer, String>>() {
					@Override
					public boolean filter(Vertex<Integer, String> value) throws Exception {
						return !value.getId().equals(3);
					}
				},
				new FilterFunction<BipartiteEdge<Integer, Integer, String>>() {
					@Override
					public boolean filter(BipartiteEdge<Integer, Integer, String> value) throws Exception {
						return !(value.getTopId().equals(5) && value.getBottomId().equals(2));
					}
				}
			);

		TestBaseUtils.compareResultAsText(bipartiteGraph.getBottomVertices().collect(),
			"(1,bottom1)\n" +
			"(2,bottom2)");

		TestBaseUtils.compareResultAsText(bipartiteGraph.getTopVertices().collect(),
			"(5,top5)\n" +
			"(6,top6)");

		TestBaseUtils.compareResultAsText(bipartiteGraph.getEdges().collect(),
			"(5,1,5-1)\n" +
			"(6,2,6-2)");
	}

	@Test
	public void testFilterOnBottomVertices() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.filterOnBottomVertices(new FilterFunction<Vertex<Integer, String>>() {
				@Override
				public boolean filter(Vertex<Integer, String> value) throws Exception {
					return !value.getId().equals(1);
				}
			});

		TestBaseUtils.compareResultAsText(bipartiteGraph.getBottomVertices().collect(),
			"(2,bottom2)\n" +
			"(3,bottom3)");

		TestBaseUtils.compareResultAsText(bipartiteGraph.getEdges().collect(),
			"(5,2,5-2)\n" +
			"(6,2,6-2)\n" +
			"(6,3,6-3)");
	}

	@Test
	public void testFilterOnEdges() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph()
			.filterOnEdges(new FilterFunction<BipartiteEdge<Integer, Integer, String>>() {
				@Override
				public boolean filter(BipartiteEdge<Integer, Integer, String> value) throws Exception {
					return !value.getBottomId().equals(1);
				}
			});

		TestBaseUtils.compareResultAsText(bipartiteGraph.getEdges().collect(),
			"(5,2,5-2)\n" +
			"(6,2,6-2)\n" +
			"(6,3,6-3)");
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

	private static class MapVertex implements MapFunction<Vertex<Integer,String>, String> {
		@Override
		public String map(Vertex<Integer, String> value) throws Exception {
			return value.getValue() + "_mapped";
		}
	}

	private static class MapEdge implements MapFunction<BipartiteEdge<Integer, Integer, String>, String> {
		@Override
		public String map(BipartiteEdge<Integer, Integer, String> value) throws Exception {
			return value.getValue() + "_mapped";
		}
	}

	private static class AppendVertexFunction implements VertexJoinFunction<String, String> {
		@Override
		public String vertexJoin(String vertexValue, String inputValue) throws Exception {
			return vertexValue + inputValue;
		}
	}

	private static class AppendEdgeFunction implements BipartiteEdgeJoinFunction<String, String> {
		@Override
        public String edgeJoin(String edgeValue, String inputValue) throws Exception {
            return edgeValue + inputValue;
        }
	}
}
