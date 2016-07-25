/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.utils.proxy;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.NoOpOperator;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link GraphAlgorithm} transforms an input {@link Graph} into an output of
 * type {@code T}. A {@code GraphAlgorithmWrappingDataSet} wraps the resultant
 * {@link Graph} vertex and edge sets with a {@code NoOpOperator}. The input to
 * the wrapped operators can be replaced when the same algorithm is run on the
 * same input with a mergeable configuration. This allows algorithms to be
 * composed of implicitly reusable algorithms without publicly sharing
 * intermediate {@link DataSet}s.
 *
 * @param <IN_K> input ID type
 * @param <IN_VV> input vertex value type
 * @param <IN_EV> input edge value type
 * @param <OUT_K> output ID type
 * @param <OUT_VV> output vertex value type
 * @param <OUT_EV> output edge value type
 */
public abstract class GraphAlgorithmWrappingGraph<IN_K, IN_VV, IN_EV, OUT_K, OUT_VV, OUT_EV>
implements GraphAlgorithm<IN_K, IN_VV, IN_EV, Graph<OUT_K, OUT_VV, OUT_EV>> {

	// each algorithm and input pair may map to multiple configurations
	private static Map<GraphAlgorithmWrappingGraph, List<GraphAlgorithmWrappingGraph>> cache =
		Collections.synchronizedMap(new HashMap<GraphAlgorithmWrappingGraph, List<GraphAlgorithmWrappingGraph>>());

	private Graph<IN_K, IN_VV, IN_EV> input;

	private NoOpOperator<Vertex<OUT_K, OUT_VV>> verticesWrappingOperator;

	private NoOpOperator<Edge<OUT_K, OUT_EV>> edgesWrappingOperator;

	/**
	 * Algorithms are identified by name rather than by class to allow subclassing.
	 *
	 * @return name of the algorithm, which may be shared by multiple classes
	 *		 implementing the same algorithm and generating the same output
	 */
	protected abstract String getAlgorithmName();

	/**
	 * An algorithm must first test whether the configurations can be merged
	 * before merging individual fields.
	 *
	 * @param other the algorithm with which to compare and merge
	 * @return true if and only if configuration has been merged and the
	 *          algorithm's output can be reused
	 */
	protected abstract boolean mergeConfiguration(GraphAlgorithmWrappingGraph other);

	/**
	 * The implementation of the algorithm, renamed from {@link GraphAlgorithm#run(Graph)}.
	 *
	 * @param input the input graph
	 * @return the algorithm's output
	 * @throws Exception
	 */
	protected abstract Graph<OUT_K, OUT_VV, OUT_EV> runInternal(Graph<IN_K, IN_VV, IN_EV> input) throws Exception;

	@Override
	public final int hashCode() {
		return new HashCodeBuilder(17, 37)
			.append(input)
			.append(getAlgorithmName())
			.toHashCode();
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		if (! GraphAlgorithmWrappingGraph.class.isAssignableFrom(obj.getClass())) {
			return false;
		}

		GraphAlgorithmWrappingGraph rhs = (GraphAlgorithmWrappingGraph) obj;

		return new EqualsBuilder()
			.append(input, rhs.input)
			.append(getAlgorithmName(), rhs.getAlgorithmName())
			.isEquals();
	}

	@Override
	@SuppressWarnings("unchecked")
	public final Graph<OUT_K, OUT_VV, OUT_EV> run(Graph<IN_K, IN_VV, IN_EV> input)
			throws Exception {
		this.input = input;

		if (cache.containsKey(this)) {
			for (GraphAlgorithmWrappingGraph<IN_K, IN_VV, IN_EV, OUT_K, OUT_VV, OUT_EV> other : cache.get(this)) {
				if (mergeConfiguration(other)) {
					// configuration has been merged so generate new output
					Graph<OUT_K, OUT_VV, OUT_EV> output = runInternal(input);

					other.verticesWrappingOperator.setInput(output.getVertices());
					other.edgesWrappingOperator.setInput(output.getEdges());

					verticesWrappingOperator = other.verticesWrappingOperator;
					edgesWrappingOperator = other.edgesWrappingOperator;

					return Graph.fromDataSet(verticesWrappingOperator, edgesWrappingOperator, output.getContext());
				}
			}
		}

		// no mergeable configuration found so generate new output
		Graph<OUT_K, OUT_VV, OUT_EV> output = runInternal(input);

		// create a new operator to wrap the algorithm output
		verticesWrappingOperator = new NoOpOperator<>(output.getVertices(), output.getVertices().getType());
		edgesWrappingOperator = new NoOpOperator<>(output.getEdges(), output.getEdges().getType());

		// cache this result
		if (cache.containsKey(this)) {
			cache.get(this).add(this);
		} else {
			cache.put(this, new ArrayList(Collections.singletonList(this)));
		}

		return Graph.fromDataSet(verticesWrappingOperator, edgesWrappingOperator, output.getContext());
	}
}
