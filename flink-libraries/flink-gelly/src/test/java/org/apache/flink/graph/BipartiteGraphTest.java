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

package org.apache.flink.graph;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.graph.generator.TestUtils.compareGraph;

public class BipartiteGraphTest {
	@Test
	public void testTopProjection() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		Graph<Integer, String, Integer> graph = bipartiteGraph.topProjection(
			new GroupReduceFunction<Tuple2<Tuple2<BipartiteEdge<Integer, Integer, String>, BipartiteEdge<Integer, Integer, String>>, Vertex<Integer,String>>, Edge<Integer, Integer>>() {
			@Override
			public void reduce(
				Iterable<Tuple2<
					Tuple2<
						BipartiteEdge<Integer, Integer, String>,
						BipartiteEdge<Integer, Integer, String>>,
					Vertex<Integer, String>>> values,
				Collector<Edge<Integer, Integer>> out) throws Exception {

				Integer source = null;
				Integer target = null;
				int count = 0;
				for (Tuple2<Tuple2<BipartiteEdge<Integer, Integer, String>, BipartiteEdge<Integer, Integer, String>>, Vertex<Integer, String>> t : values) {
					source = t.f0.f0.getTopId();
					target = t.f0.f1.getTopId();
					count++;
				}

				if (source.compareTo(target) > 0)
					out.collect(new Edge<Integer, Integer>(source, target, count));
			}
		});

		compareGraph(graph, "4; 5; 6", "5,4; 6,5");
	}

	@Test
	public void testBottomProjection() throws Exception {
		BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph = createBipartiteGraph();

		Graph<Integer, String, Integer> graph = bipartiteGraph.bottomProjection(
			new GroupReduceFunction<Tuple2<Tuple2<BipartiteEdge<Integer, Integer, String>, BipartiteEdge<Integer, Integer, String>>, Vertex<Integer,String>>, Edge<Integer, Integer>>() {
				@Override
				public void reduce(
					Iterable<Tuple2<
						Tuple2<
							BipartiteEdge<Integer, Integer, String>,
							BipartiteEdge<Integer, Integer, String>>,
						Vertex<Integer, String>>> values,
					Collector<Edge<Integer, Integer>> out) throws Exception {

					Integer source = null;
					Integer target = null;
					int count = 0;
					for (Tuple2<Tuple2<BipartiteEdge<Integer, Integer, String>, BipartiteEdge<Integer, Integer, String>>, Vertex<Integer, String>> t : values) {
						source = t.f0.f0.getBottomId();
						target = t.f0.f1.getBottomId();
						count++;
					}

					if (source.compareTo(target) > 0)
						out.collect(new Edge<Integer, Integer>(source, target, count));
				}
			});

		compareGraph(graph, "1; 2; 3", "2,1; 3,2");
	}

	private BipartiteGraph<Integer, Integer, String, String, String> createBipartiteGraph() {
		ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Integer, String>> bottomVertices = executionEnvironment.fromCollection(Arrays.asList(
			new Vertex<>(1, "bottom1"),
			new Vertex<>(2, "bottom2"),
			new Vertex<>(3, "bottom3")
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
			new BipartiteEdge<>(6, 3, "6-3")
		));

		return BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, executionEnvironment);
	}
}
