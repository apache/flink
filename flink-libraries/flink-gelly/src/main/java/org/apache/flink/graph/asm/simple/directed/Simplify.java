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

package org.apache.flink.graph.asm.simple.directed;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmDelegatingGraph;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Remove self-loops and duplicate edges from a directed graph.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class Simplify<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends GraphAlgorithmDelegatingGraph<K, VV, EV, K, VV, EV> {

	// Optional configuration
	private int parallelism = PARALLELISM_DEFAULT;

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public Simplify<K, VV, EV> setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT,
			"The parallelism must be greater than zero.");

		this.parallelism = parallelism;

		return this;
	}

	@Override
	protected String getAlgorithmName() {
		return Simplify.class.getName();
	}

	@Override
	protected boolean mergeConfiguration(GraphAlgorithmDelegatingGraph other) {
		Preconditions.checkNotNull(other);

		if (! Simplify.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		Simplify rhs = (Simplify) other;

		parallelism = Math.min(parallelism, rhs.parallelism);

		return true;
	}

	@Override
	public Graph<K, VV, EV> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// Edges
		DataSet<Edge<K, EV>> edges = input
			.getEdges()
			.filter(new RemoveSelfLoops<K, EV>())
				.setParallelism(parallelism)
				.name("Remove self-loops")
			.distinct(0, 1)
				.setParallelism(parallelism)
				.name("Remove duplicate edges");

		// Graph
		return Graph.fromDataSet(input.getVertices(), edges, input.getContext());
	}

	/**
	 * Filter out edges where the source and target vertex IDs are equal.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	private static class RemoveSelfLoops<T extends Comparable<T>, ET>
	implements FilterFunction<Edge<T, ET>> {
		@Override
		public boolean filter(Edge<T, ET> value) throws Exception {
			return (value.f0.compareTo(value.f1) != 0);
		}
	}
}
