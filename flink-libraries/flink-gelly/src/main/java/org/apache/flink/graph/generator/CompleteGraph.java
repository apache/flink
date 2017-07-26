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
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Preconditions;

/**
 * @see <a href="http://mathworld.wolfram.com/CompleteGraph.html">Complete Graph at Wolfram MathWorld</a>
 */
public class CompleteGraph
extends GraphGeneratorBase<LongValue, NullValue, NullValue> {

	public static final int MINIMUM_VERTEX_COUNT = 2;

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private final long vertexCount;

	/**
	 * An undirected {@link Graph} connecting every distinct pair of vertices.
	 *
	 * @param env the Flink execution environment
	 * @param vertexCount number of vertices
	 */
	public CompleteGraph(ExecutionEnvironment env, long vertexCount) {
		Preconditions.checkArgument(vertexCount >= MINIMUM_VERTEX_COUNT,
			"Vertex count must be at least " + MINIMUM_VERTEX_COUNT);

		this.env = env;
		this.vertexCount = vertexCount;
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> generate() {
		return new CirculantGraph(env, vertexCount)
			.addRange(1, vertexCount - 1)
				.setParallelism(parallelism)
				.generate();
	}
}
