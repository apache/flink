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
 * Provides the default data set used for the Label Propagation test program.
 * If no parameters are given to the program, the default edge data set is used.
 */
public class LabelPropagationData {

	public static final String LABELS_AFTER_1_ITERATION = "1,10\n" +
			"2,10\n" +
			"3,10\n" +
			"4,40\n" +
			"5,40\n" +
			"6,40\n" +
			"7,40\n";

	public static final String LABELS_WITH_TIE = "1,10\n" +
			"2,10\n" +
			"3,10\n" +
			"4,10\n" +
			"5,20\n" +
			"6,20\n" +
			"7,20\n" +
			"8,20\n" +
			"9,20\n";

	private LabelPropagationData() {}

	public static final DataSet<Vertex<Long, Long>> getDefaultVertexSet(ExecutionEnvironment env) {

		List<Vertex<Long, Long>> vertices = new ArrayList<>();
		vertices.add(new Vertex<>(1L, 10L));
		vertices.add(new Vertex<>(2L, 10L));
		vertices.add(new Vertex<>(3L, 30L));
		vertices.add(new Vertex<>(4L, 40L));
		vertices.add(new Vertex<>(5L, 40L));
		vertices.add(new Vertex<>(6L, 40L));
		vertices.add(new Vertex<>(7L, 40L));

		return env.fromCollection(vertices);
	}

	public static final DataSet<Edge<Long, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(2L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(5L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(6L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(7L, 3L, NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static final DataSet<Vertex<Long, Long>> getTieVertexSet(ExecutionEnvironment env) {

		List<Vertex<Long, Long>> vertices = new ArrayList<>();
		vertices.add(new Vertex<>(1L, 10L));
		vertices.add(new Vertex<>(2L, 10L));
		vertices.add(new Vertex<>(3L, 10L));
		vertices.add(new Vertex<>(4L, 10L));
		vertices.add(new Vertex<>(5L, 0L));
		vertices.add(new Vertex<>(6L, 20L));
		vertices.add(new Vertex<>(7L, 20L));
		vertices.add(new Vertex<>(8L, 20L));
		vertices.add(new Vertex<>(9L, 20L));

		return env.fromCollection(vertices);
	}

	public static final DataSet<Edge<Long, NullValue>> getTieEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(2L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(5L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(6L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(7L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(8L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(9L, 5L, NullValue.getInstance()));

		return env.fromCollection(edges);
	}
}

