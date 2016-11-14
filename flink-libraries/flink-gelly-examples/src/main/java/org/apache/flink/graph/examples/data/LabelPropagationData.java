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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

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

	public static final String LABELS_WITH_TIE ="1,10\n" +
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

		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		vertices.add(new Vertex<Long, Long>(1l, 10l));
		vertices.add(new Vertex<Long, Long>(2l, 10l));
		vertices.add(new Vertex<Long, Long>(3l, 30l));
		vertices.add(new Vertex<Long, Long>(4l, 40l));
		vertices.add(new Vertex<Long, Long>(5l, 40l));
		vertices.add(new Vertex<Long, Long>(6l, 40l));
		vertices.add(new Vertex<Long, Long>(7l, 40l));

		return env.fromCollection(vertices);
	}

	public static final DataSet<Edge<Long, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, NullValue>> edges = new ArrayList<Edge<Long, NullValue>>();
		edges.add(new Edge<Long, NullValue>(1L, 3L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(2L, 3L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(4L, 7L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(5L, 7L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(6L, 7L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(7L, 3L, NullValue.getInstance()));

		return env.fromCollection(edges);
	}

	public static final DataSet<Vertex<Long, Long>> getTieVertexSet(ExecutionEnvironment env) {

		List<Vertex<Long, Long>> vertices = new ArrayList<Vertex<Long, Long>>();
		vertices.add(new Vertex<Long, Long>(1l, 10l));
		vertices.add(new Vertex<Long, Long>(2l, 10l));
		vertices.add(new Vertex<Long, Long>(3l, 10l));
		vertices.add(new Vertex<Long, Long>(4l, 10l));
		vertices.add(new Vertex<Long, Long>(5l, 0l));
		vertices.add(new Vertex<Long, Long>(6l, 20l));
		vertices.add(new Vertex<Long, Long>(7l, 20l));
		vertices.add(new Vertex<Long, Long>(8l, 20l));
		vertices.add(new Vertex<Long, Long>(9l, 20l));

		return env.fromCollection(vertices);
	}

	public static final DataSet<Edge<Long, NullValue>> getTieEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, NullValue>> edges = new ArrayList<Edge<Long, NullValue>>();
		edges.add(new Edge<Long, NullValue>(1L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(2L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(4L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(5L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(6L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(7L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(8L, 5L, NullValue.getInstance()));
		edges.add(new Edge<Long, NullValue>(9L, 5L, NullValue.getInstance()));

		return env.fromCollection(edges);
	}
}

