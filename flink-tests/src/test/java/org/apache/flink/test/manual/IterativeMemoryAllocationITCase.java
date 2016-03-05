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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a performance test for FLINK-3322.
 * This test has little data and high number of iterations in order to measure the overhead of the iterations.
 * The test uses the connected component algorithm. It is based on the ConnectedComponents test.
 */
public class IterativeMemoryAllocationITCase {
	@SuppressWarnings("serial")
	public static void main(String [] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(edges, new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return value;
			}
		}, env);

		DataSet<Vertex<Long, Long>> verticesWithMinIds = graph
			.run(new GSAConnectedComponents<Long, NullValue>(maxIterations));

		System.out.println("start");
		long start = System.currentTimeMillis();

		// emit result
		verticesWithMinIds.print();

		long end = System.currentTimeMillis();
		System.out.println("stop, time: " + (end - start));
	}

	private static Integer maxIterations = 4000000;

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		for(long i=0; i<1000; i++){
			edges.add(new Edge<Long, NullValue>(i, i+1, NullValue.getInstance()));
		}
		return env.fromCollection(edges);
	}
}

