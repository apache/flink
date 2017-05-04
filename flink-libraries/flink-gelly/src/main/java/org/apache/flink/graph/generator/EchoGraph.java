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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Preconditions;

/**
 * Every {@link Vertex} in the {@link EchoGraph} has the same degree.
 * and vertices as far as possible are chose to be linked.
 * {@link EchoGraph} is a specific case of {@link CirculantGraph}.
 */
public class EchoGraph
extends AbstractGraphGenerator<LongValue, NullValue, NullValue> {

	public static final int MINIMUM_VERTEX_COUNT = 1;

	public static final int MINIMUM_VERTEX_DEGREE = 0;

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private long vertexCount;

	private long vertexDegree;

	/**
	 * An undirected {@link Graph} whose vertices have the same degree.
	 *
	 * @param env the Flink execution environment
	 * @param vertexCount number of vertices
	 * @param vertexDegree degree of vertices
	 */
	public EchoGraph(ExecutionEnvironment env, long vertexCount, long vertexDegree) {
		Preconditions.checkArgument(vertexCount >= MINIMUM_VERTEX_COUNT,
			"Vertex count must be at least " + MINIMUM_VERTEX_COUNT);
		Preconditions.checkArgument(vertexDegree >= MINIMUM_VERTEX_DEGREE,
				"Vertex degree must be at least " + MINIMUM_VERTEX_DEGREE);
		Preconditions.checkArgument(vertexDegree <= vertexCount - 1,
				"Vertex degree must be at most " + (vertexCount - 1));
		Preconditions.checkArgument(vertexCount % 2 == 0 || vertexDegree % 2 == 0,
				"Vertex degree must be even when vertex count is odd number");

		this.env = env;
		this.vertexCount = vertexCount;
		this.vertexDegree = vertexDegree;
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> generate() {
		CirculantGraph circulantGraph = new CirculantGraph(env, vertexCount);
		long maxOffset = vertexCount / 2;

		// add max offset when vertex degree is even and vertex count is odd
		if (vertexDegree % 2 == 1 && vertexCount % 2 == 0) {
			circulantGraph.addOffsetRange(maxOffset, 1);
		}

		// add other offset nearby max offset
		long length = vertexDegree / 2;
		final long startOffset = maxOffset - length + vertexCount % 2;
		circulantGraph.addOffsetRange(startOffset, length);

		return circulantGraph
				.setParallelism(parallelism)
				.generate();
	}
}
