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

package org.apache.flink.graph.example.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import java.util.ArrayList;
import java.util.List;

public class SingleSourceShortestPathsData {

	public static final int NUM_VERTICES = 5;

	public static final Long SRC_VERTEX_ID = 1L;

	public static final String VERTICES = "1,1.0\n" + "2,2.0\n" + "3,3.0\n" + "4,4.0\n" + "5,5.0";

	public static DataSet<Vertex<Long, Double>> getDefaultVertexDataSet(ExecutionEnvironment env) {

		List<Vertex<Long, Double>> vertices = new ArrayList<Vertex<Long, Double>>();
		vertices.add(new Vertex<Long, Double>(1L, 1.0));
		vertices.add(new Vertex<Long, Double>(2L, 2.0));
		vertices.add(new Vertex<Long, Double>(3L, 3.0));
		vertices.add(new Vertex<Long, Double>(4L, 4.0));
		vertices.add(new Vertex<Long, Double>(5L, 5.0));

		return env.fromCollection(vertices);
	}

	public static final String EDGES = "1,2,12.0\n" + "1,3,13.0\n" + "2,3,23.0\n" + "3,4,34.0\n" + "3,5,35.0\n" + 
					"4,5,45.0\n" + "5,1,51.0";

	public static final DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, Double>> edges = new ArrayList<Edge<Long, Double>>();
		edges.add(new Edge<Long, Double>(1L, 2L, 12.0));
		edges.add(new Edge<Long, Double>(1L, 3L, 13.0));
		edges.add(new Edge<Long, Double>(2L, 3L, 23.0));
		edges.add(new Edge<Long, Double>(3L, 4L, 34.0));
		edges.add(new Edge<Long, Double>(3L, 5L, 35.0));
		edges.add(new Edge<Long, Double>(4L, 5L, 45.0));
		edges.add(new Edge<Long, Double>(5L, 1L, 51.0));

		return env.fromCollection(edges);
	}

	public static final String RESULTED_SINGLE_SOURCE_SHORTEST_PATHS =  "1,0.0\n" + "2,12.0\n" + "3,13.0\n" + 
								"4,47.0\n" + "5,48.0";

	private SingleSourceShortestPathsData() {}
}
