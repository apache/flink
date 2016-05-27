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

package org.apache.flink.graph.examples.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the data set used for the HITS test program.
 */
public class HITSData {

	public static final String VALUE_AFTER_10_ITERATIONS = "1,0.70710678,3.12608866E-8\n" +
															"2,1.29486832E-8,0.70710678\n" +
															"3,1.29486832E-8,0.49999999\n" +
															"4,0.50000001,0.49999999\n" +
															"5,0.50000001,3.12608866E-8\n";


	private HITSData() {}

	public static final DataSet<Vertex<Long, Double>> getVertexDataSet(ExecutionEnvironment env) {

		List<Vertex<Long, Double>> vertices = new ArrayList<>();
		vertices.add(new Vertex<>(1L, 1.0));
		vertices.add(new Vertex<>(2L, 2.0));
		vertices.add(new Vertex<>(3L, 3.0));
		vertices.add(new Vertex<>(4L, 4.0));
		vertices.add(new Vertex<>(5L, 5.0));
	
		return env.fromCollection(vertices);
	}	
	
	public static final DataSet<Edge<Long, NullValue>> getEdgeDataSet(ExecutionEnvironment env) {
		
		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(2L, 1L, NullValue.getInstance()));
		edges.add(new Edge<>(5L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(5L, 4L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 4L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(3L, 5L, NullValue.getInstance()));
		
		return env.fromCollection(edges);
	}
}
