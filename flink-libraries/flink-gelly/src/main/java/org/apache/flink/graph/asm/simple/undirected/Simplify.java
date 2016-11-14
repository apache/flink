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

package org.apache.flink.graph.asm.simple.undirected;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingGraph;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Add symmetric edges and remove self-loops and duplicate edges from an
 * undirected graph.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class Simplify<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends GraphAlgorithmWrappingGraph<K, VV, EV, K, VV, EV> {

	// Required configuration
	private boolean clipAndFlip;

	// Optional configuration
	private int parallelism = PARALLELISM_DEFAULT;

	/**
	 * Simplifies an undirected graph by adding reverse edges and removing
	 * self-loops and duplicate edges.
	 *
	 * When clip-and-flip is set, edges where source < target are removed
	 * before symmetrizing the graph.
	 *
	 * @param clipAndFlip method for generating simple graph
	 */
	public Simplify(boolean clipAndFlip) {
		this.clipAndFlip = clipAndFlip;
	}

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
	protected boolean mergeConfiguration(GraphAlgorithmWrappingGraph other) {
		Preconditions.checkNotNull(other);

		if (! Simplify.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		Simplify rhs = (Simplify) other;

		// verify that configurations can be merged

		if (clipAndFlip != rhs.clipAndFlip) {
			return false;
		}

		// merge configurations

		parallelism = (parallelism == PARALLELISM_DEFAULT) ? rhs.parallelism :
			((rhs.parallelism == PARALLELISM_DEFAULT) ? parallelism : Math.min(parallelism, rhs.parallelism));

		return true;
	}

	@Override
	public Graph<K, VV, EV> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		// Edges
		DataSet<Edge<K, EV>> edges = input
			.getEdges()
			.flatMap(new SymmetrizeAndRemoveSelfLoops<K, EV>(clipAndFlip))
				.setParallelism(parallelism)
				.name("Remove self-loops")
			.distinct(0, 1)
				.setParallelism(parallelism)
				.name("Remove duplicate edges");

		// Graph
		return Graph.fromDataSet(input.getVertices(), edges, input.getContext());
	}

	/**
	 * Filter out edges where the source and target vertex IDs are equal and
	 * for each edge also emit an edge with the vertex IDs flipped.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	private static class SymmetrizeAndRemoveSelfLoops<T extends Comparable<T>, ET>
	implements FlatMapFunction<Edge<T, ET>, Edge<T, ET>> {
		private boolean clipAndFlip;

		public SymmetrizeAndRemoveSelfLoops(boolean clipAndFlip) {
			this.clipAndFlip = clipAndFlip;
		}

		@Override
		public void flatMap(Edge<T, ET> value, Collector<Edge<T, ET>> out) throws Exception {
			int comparison = value.f0.compareTo(value.f1);

			if ((clipAndFlip && comparison > 0) || (!clipAndFlip && comparison != 0)) {
				out.collect(value);

				T temp = value.f0;
				value.f0 = value.f1;
				value.f1 = temp;

				out.collect(value);
			}
		}
	}
}
