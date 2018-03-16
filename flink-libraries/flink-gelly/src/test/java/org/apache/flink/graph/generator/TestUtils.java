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

package org.apache.flink.graph.generator;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.test.util.TestBaseUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Utility methods for testing graph algorithms.
 */
public final class TestUtils {

	private TestUtils() {}

	/**
	 * Compare graph vertices and edges against expected values.
	 *
	 * @param graph graph under test
	 * @param expectedVertices vertex labels separated by semi-colons; whitespace is ignored
	 * @param expectedEdges edges of the form "source,target" separated by semi-colons; whitespace is ignored
	 * @param <K> the key type for edge and vertex identifiers
	 * @param <VV> the value type for vertices
	 * @param <EV> the value type for edges
	 * @throws Exception
	 */
	public static <K, VV, EV> void compareGraph(Graph<K, VV, EV> graph, String expectedVertices, String expectedEdges) throws Exception {
		compareVertices(graph, expectedVertices);
		compareEdges(graph, expectedEdges);
	}

	private static <K, VV, EV> void compareVertices(Graph<K, VV, EV> graph, String expectedVertices) throws Exception {
		if (expectedVertices != null) {
			List<Vertex<K, VV>> vertices = graph.getVertices().collect();
			List<String> resultVertices = new ArrayList<>(vertices.size());

			for (Vertex<K, VV> vertex : vertices) {
				resultVertices.add(vertex.f0.toString());
			}

			TestBaseUtils.compareResultAsText(resultVertices, expectedVertices.replaceAll("\\s", "").replace(";", "\n"));
		}
	}

	private static <K, VV, EV> void compareEdges(Graph<K, VV, EV> graph, String expectedEdges) throws Exception {
		if (expectedEdges != null) {
			List<Edge<K, EV>> edges = graph.getEdges().collect();
			List<String> resultEdges = new ArrayList<>(edges.size());

			for (Edge<K, EV> edge : edges) {
				resultEdges.add(edge.f0.toString() + "," + edge.f1.toString());
			}

			TestBaseUtils.compareResultAsText(resultEdges, expectedEdges.replaceAll("\\s", "").replace(";", "\n"));
		}
	}

	/**
	 * Verify operator parallelism.
	 *
	 * @param env the Flink execution environment.
	 * @param expectedParallelism expected operator parallelism
	 */
	public static void verifyParallelism(ExecutionEnvironment env, int expectedParallelism) {
		env.setParallelism(2 * expectedParallelism);

		Optimizer compiler = new Optimizer(null, new DefaultCostEstimator(), new Configuration());
		OptimizedPlan optimizedPlan = compiler.compile(env.createProgramPlan());

		List<PlanNode> queue = new ArrayList<>();
		queue.addAll(optimizedPlan.getDataSinks());

		while (queue.size() > 0) {
			PlanNode node = queue.remove(queue.size() - 1);

			// Data sources may have parallelism of 1, so simply check that the node
			// parallelism has not been increased by setting the default parallelism
			assertTrue("Wrong parallelism for " + node.toString(), node.getParallelism() <= expectedParallelism);

			for (Channel channel : node.getInputs()) {
				queue.add(channel.getSource());
			}
		}
	}
}
