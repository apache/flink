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

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides the default data sets used for the Minimum Spanning Tree example program.
 * If no parameters are given to the program, the default data sets are used.
 */
public class MinSpanningTreeData {

	public static final int NUM_VERTICES = 12;

	public static final String VERTICES = "1\n" + "2\n" + "3\n" + "4\n" + "5\n" + "6\n" + "7\n" + "8\n" + "9\n" + "10\n" +
						"11\n" + "12";

	public static DataSet<Vertex<Long, String>> getDefaultVertexDataSet(ExecutionEnvironment env) {

		List<Vertex<Long, String>> vertices = new ArrayList<Vertex<Long, String>>();
		vertices.add(new Vertex<Long, String>(1L, "A"));
		vertices.add(new Vertex<Long, String>(2L, "B"));
		vertices.add(new Vertex<Long, String>(3L, "C"));
		vertices.add(new Vertex<Long, String>(4L, "D"));
		vertices.add(new Vertex<Long, String>(5L, "E"));
		vertices.add(new Vertex<Long, String>(6L, "F"));
		vertices.add(new Vertex<Long, String>(7L, "G"));
		vertices.add(new Vertex<Long, String>(8L, "H"));
		vertices.add(new Vertex<Long, String>(9L, "I"));
		vertices.add(new Vertex<Long, String>(10L, "J"));
		vertices.add(new Vertex<Long, String>(11L, "K"));
		vertices.add(new Vertex<Long, String>(12L, "L"));

		return env.fromCollection(vertices);
	}

	public static final String EDGES = "1,2,13.0\n" + "1,3,6.0\n" + "2,3,7.0\n" + "2,4,1.0\n" + "3,4,14.0\n" + "3,5,8.0\n"
				+ "3,8,20.0\n" + "4,5,9.0\n" + "4,6,3.0\n" + "5,6,2.0\n" +"5,10,18.0\n" + "7,8,15.0\n" + "7,9,5.0\n"
				+ "7,10,19.0\n" + "7,11,10.0\n" + "8,10,17.0\n" + "9,11,11.0\n" + "10,11,16.0\n" + "10,12,4.0\n" + "11,12,12.0";

	public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, Double>> edges = new ArrayList<Edge<Long, Double>>();
		edges.add(new Edge<Long, Double>(1L, 2L, 13.0));
		edges.add(new Edge<Long, Double>(1L, 3L, 6.0));
		edges.add(new Edge<Long, Double>(2L, 3L, 7.0));
		edges.add(new Edge<Long, Double>(2L, 4L, 1.0));
		edges.add(new Edge<Long, Double>(3L, 4L, 14.0));
		edges.add(new Edge<Long, Double>(3L, 5L, 8.0));
		edges.add(new Edge<Long, Double>(3L, 8L, 20.0));
		edges.add(new Edge<Long, Double>(4L, 5L, 9.0));
		edges.add(new Edge<Long, Double>(4L, 6L, 3.0));
		edges.add(new Edge<Long, Double>(5L, 6L, 2.0));
		edges.add(new Edge<Long, Double>(5L, 10L, 18.0));
		edges.add(new Edge<Long, Double>(7L, 8L, 15.0));
		edges.add(new Edge<Long, Double>(7L, 9L, 5.0));
		edges.add(new Edge<Long, Double>(7L, 10L, 19.0));
		edges.add(new Edge<Long, Double>(7L, 11L, 10.0));
		edges.add(new Edge<Long, Double>(8L, 10L, 17.0));
		edges.add(new Edge<Long, Double>(9L, 11L, 11.0));
		edges.add(new Edge<Long, Double>(10L, 11L, 16.0));
		edges.add(new Edge<Long, Double>(10L, 12L, 4.0));
		edges.add(new Edge<Long, Double>(11L, 12L, 12.0));

		return env.fromCollection(edges);
	}

	public static final String RESULTED_MIN_SPANNING_TREE = "1,3,6.0\n" + "2,3,7.0\n" + "2,4,1.0\n"
		+ "4,6,3.0\n" + "5,6,2.0\n" + "5,10,18.0\n" + "7,8,15.0\n" + "7,9,5.0\n" + "7,11,10.0\n" + "10,12,4.0\n"
		+ "11,12,12.0";

	private MinSpanningTreeData() {}
}
