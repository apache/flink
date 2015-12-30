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

import com.google.common.collect.Lists;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.List;

/**
 * Provides the default data set used for Summarization tests.
 */
public class SummarizationData {

	private SummarizationData() {}

	/**
	 * The resulting vertex id can be any id of the vertices summarized by the single vertex.
	 *
	 * Format:
	 *
	 * "possible-id[,possible-id];group-value,group-count"
	 */
	public static final String[] EXPECTED_VERTICES = new String[] {
			"0,1;A,2",
			"2,3,4;B,3",
			"5;C,1"
	};

	/**
	 * Format:
	 *
	 * "possible-source-id[,possible-source-id];possible-target-id[,possible-target-id];group-value,group-count"
	 */
	public static final String[] EXPECTED_EDGES_WITH_VALUES = new String[] {
			"0,1;0,1;A,2",
			"0,1;2,3,4;A,1",
			"2,3,4;0,1;A,1",
			"2,3,4;0,1;C,2",
			"2,3,4;2,3,4;B,2",
			"5;2,3,4;D,2"
	};

	/**
	 * Format:
	 *
	 * "possible-source-id[,possible-source-id];possible-target-id[,possible-target-id];group-value,group-count"
	 */
	public static final String[] EXPECTED_EDGES_ABSENT_VALUES = new String[] {
			"0,1;0,1;(null),2",
			"0,1;2,3,4;(null),1",
			"2,3,4;0,1;(null),3",
			"2,3,4;2,3,4;(null),2",
			"5;2,3,4;(null),2"
	};

	/**
	 * Creates a set of vertices with attached {@link String} values.
	 *
	 * @param env execution environment
	 * @return vertex data set with string values
	 */
	public static DataSet<Vertex<Long, String>> getVertices(ExecutionEnvironment env) {
		List<Vertex<Long, String>> vertices = Lists.newArrayListWithExpectedSize(6);
		vertices.add(new Vertex<>(0L, "A"));
		vertices.add(new Vertex<>(1L, "A"));
		vertices.add(new Vertex<>(2L, "B"));
		vertices.add(new Vertex<>(3L, "B"));
		vertices.add(new Vertex<>(4L, "B"));
		vertices.add(new Vertex<>(5L, "C"));

		return env.fromCollection(vertices);
	}

	/**
	 * Creates a set of edges with attached {@link String} values.
	 *
	 * @param env execution environment
	 * @return edge data set with string values
	 */
	public static DataSet<Edge<Long, String>> getEdges(ExecutionEnvironment env) {
		List<Edge<Long, String>> edges = Lists.newArrayListWithExpectedSize(10);
		edges.add(new Edge<>(0L, 1L, "A"));
		edges.add(new Edge<>(1L, 0L, "A"));
		edges.add(new Edge<>(1L, 2L, "A"));
		edges.add(new Edge<>(2L, 1L, "A"));
		edges.add(new Edge<>(2L, 3L, "B"));
		edges.add(new Edge<>(3L, 2L, "B"));
		edges.add(new Edge<>(4L, 0L, "C"));
		edges.add(new Edge<>(4L, 1L, "C"));
		edges.add(new Edge<>(5L, 2L, "D"));
		edges.add(new Edge<>(5L, 3L, "D"));

		return env.fromCollection(edges);
	}

	/**
	 * Creates a set of edges with {@link NullValue} as edge value.
	 *
	 * @param env execution environment
	 * @return edge data set with null values
	 */
	@SuppressWarnings("serial")
	public static DataSet<Edge<Long, NullValue>> getEdgesWithAbsentValues(ExecutionEnvironment env) {
		return getEdges(env).map(new MapFunction<Edge<Long, String>, Edge<Long, NullValue>>() {
			@Override
			public Edge<Long, NullValue> map(Edge<Long, String> value) throws Exception {
				return new Edge<>(value.getSource(), value.getTarget(), NullValue.getInstance());
			}
		});
	}
}
