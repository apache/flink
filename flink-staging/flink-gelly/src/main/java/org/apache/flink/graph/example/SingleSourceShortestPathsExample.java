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

package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.ExampleUtils;
import org.apache.flink.graph.library.SingleSourceShortestPaths;

public class SingleSourceShortestPathsExample implements ProgramDescription {

	private static int maxIterations = 5;

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, Double>> vertices = ExampleUtils.getLongDoubleVertexData(env);

		DataSet<Edge<Long, Double>> edges = ExampleUtils.getLongDoubleEdgeData(env);

		Long srcVertexId = 1L;

		Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = graph
				.run(new SingleSourceShortestPaths<Long>(srcVertexId,
						maxIterations)).getVertices();

		singleSourceShortestPaths.print();

		env.execute();
	}

	@Override
	public String getDescription() {
		return "Single Source Shortest Paths";
	}
}
