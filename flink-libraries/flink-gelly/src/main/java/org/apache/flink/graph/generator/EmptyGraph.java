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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.util.Collections;

/*
 * @see <a href="http://mathworld.wolfram.com/EmptyGraph.html">Empty Graph at Wolfram MathWorld</a>
 */
public class EmptyGraph
extends AbstractGraphGenerator<LongValue, NullValue, NullValue> {

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private long vertexCount;

	/**
	 * The {@link Graph} containing no edges.
	 *
	 * @param env the Flink execution environment
	 * @param vertexCount number of vertices
	 */
	public EmptyGraph(ExecutionEnvironment env, long vertexCount) {
		if (vertexCount <= 0) {
			throw new IllegalArgumentException("Vertex count must be greater than zero");
		}

		this.env = env;
		this.vertexCount = vertexCount;
	}

	@Override
	public Graph<LongValue,NullValue,NullValue> generate() {
		// Vertices
		DataSet<Vertex<LongValue,NullValue>> vertices = GraphGeneratorUtils.vertexSequence(env, parallelism, vertexCount);

		// Edges
		TypeInformation<Edge<LongValue,NullValue>> typeInformation = new TupleTypeInfo<>(
			ValueTypeInfo.LONG_VALUE_TYPE_INFO, ValueTypeInfo.LONG_VALUE_TYPE_INFO, ValueTypeInfo.NULL_VALUE_TYPE_INFO);

		DataSource<Edge<LongValue,NullValue>> edges = env
			.fromCollection(Collections.<Edge<LongValue,NullValue>>emptyList(), typeInformation)
				.setParallelism(parallelism)
				.name("Empty edge set");

		// Graph
		return Graph.fromDataSet(vertices, edges, env);
	}
}
