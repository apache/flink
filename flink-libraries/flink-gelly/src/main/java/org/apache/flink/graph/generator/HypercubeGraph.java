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

/*
 * @see <a href="http://mathworld.wolfram.com/HypercubeGraph.html">Hypercube Graph at Wolfram MathWorld</a>
 */
public class HypercubeGraph
extends AbstractGraphGenerator<LongValue, NullValue, NullValue> {

	// Required to create the DataSource
	private final ExecutionEnvironment env;

	// Required configuration
	private long dimensions;

	/**
	 * An undirected {@link Graph} where edges form an n-dimensional hypercube.
	 *
	 * @param env the Flink execution environment
	 * @param dimensions number of dimensions
	 */
	public HypercubeGraph(ExecutionEnvironment env, long dimensions) {
		if (dimensions <= 0) {
			throw new IllegalArgumentException("Number of dimensions must be greater than zero");
		}

		this.env = env;
		this.dimensions = dimensions;
	}

	@Override
	public Graph<LongValue, NullValue, NullValue> generate() {
		GridGraph graph = new GridGraph(env);

		for (int i = 0; i < dimensions; i++) {
			graph.addDimension(2, false);
		}

		return graph
			.setParallelism(parallelism)
			.generate();
	}
}
