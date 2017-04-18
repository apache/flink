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
 * Provides the default data set used for the Single Source Shortest Paths example program.
 * If no parameters are given to the program, the default edge data set is used.
 */
public class AffinityPropagationData {

	//public static final Integer MAX_ITERATIONS = 4;

	public static List<Vertex<Long, NullValue>> getLongLongVertices() {
		List<Vertex<Long, NullValue>> vertices = new ArrayList<>();
		vertices.add(new Vertex<>(1L, NullValue.getInstance()));
		vertices.add(new Vertex<>(2L, NullValue.getInstance()));
		vertices.add(new Vertex<>(3L, NullValue.getInstance()));

		return vertices;
	}

	public static List<Edge<Long, Double>> getLongLongEdges() {
		List<Edge<Long, Double>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 1L, 1.0));
		edges.add(new Edge<>(1L, 2L, 1.0));
		edges.add(new Edge<>(1L, 3L, 5.0));
		edges.add(new Edge<>(2L, 1L, 1.0));
		edges.add(new Edge<>(2L, 2L, 1.0));
		edges.add(new Edge<>(2L, 3L, 3.0));
		edges.add(new Edge<>(3L, 1L, 5.0));
		edges.add(new Edge<>(3L, 2L, 3.0));
		edges.add(new Edge<>(3L, 3L, 1.0));

		return edges;
	}

	public static double[][] getArray() {
		double[][] data = new double[][]{
			{ 1.0, 1.0, 5.0},
			{ 1.0, 1.0, 3.0},
			{ 5.0, 3.0, 1.0}
		};

		return data;
	}

	public static DataSet<Vertex<Long, NullValue>> getLongLongVertexData(
		ExecutionEnvironment env) {

		return env.fromCollection(getLongLongVertices());
	}

	public static DataSet<Edge<Long, Double>> getLongLongEdgeData(
		ExecutionEnvironment env) {

		return env.fromCollection(getLongLongEdges());
	}

	private AffinityPropagationData() {}
}
