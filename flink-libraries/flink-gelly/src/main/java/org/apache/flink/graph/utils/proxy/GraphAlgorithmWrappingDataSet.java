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

package org.apache.flink.graph.utils.proxy;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.NoOpOperator;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link GraphAlgorithm} transforms an input {@link Graph} into an output of
 * type {@code T}. A {@code GraphAlgorithmWrappingDataSet} wraps the resultant
 * {@link DataSet} with a {@code NoOpOperator}. The input to the wrapped
 * operator can be replaced when the same algorithm is run on the same input
 * with a mergeable configuration. This allows algorithms to be composed of
 * implicitly reusable algorithms without publicly sharing intermediate
 * {@link DataSet}s.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @param <T> output type
 */
public abstract class GraphAlgorithmWrappingDataSet<K, VV, EV, T>
implements GraphAlgorithm<K, VV, EV, DataSet<T>> {

	// each algorithm and input pair may map to multiple configurations
	private static Map<GraphAlgorithmWrappingDataSet, List<GraphAlgorithmWrappingDataSet>> cache =
		Collections.synchronizedMap(new HashMap<GraphAlgorithmWrappingDataSet, List<GraphAlgorithmWrappingDataSet>>());

	private Graph<K, VV, EV> input;

	private NoOpOperator<T> wrappingOperator;

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
	protected abstract boolean mergeConfiguration(GraphAlgorithmWrappingDataSet other);

	/**
	 * The implementation of the algorithm, renamed from {@link GraphAlgorithm#run(Graph)}.
	 *
	 * @param input the input graph
	 * @return the algorithm's output
	 * @throws Exception
	 */
	protected abstract DataSet<T> runInternal(Graph<K, VV, EV> input) throws Exception;

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

		if (! GraphAlgorithmWrappingDataSet.class.isAssignableFrom(obj.getClass())) {
			return false;
		}

		GraphAlgorithmWrappingDataSet rhs = (GraphAlgorithmWrappingDataSet) obj;

		return new EqualsBuilder()
			.append(input, rhs.input)
			.append(getAlgorithmName(), rhs.getAlgorithmName())
			.isEquals();
	}

	@Override
	@SuppressWarnings("unchecked")
	public final DataSet<T> run(Graph<K, VV, EV> input)
			throws Exception {
		this.input = input;

		if (cache.containsKey(this)) {
			for (GraphAlgorithmWrappingDataSet<K, VV, EV, T> other : cache.get(this)) {
				if (mergeConfiguration(other)) {
					// configuration has been merged so generate new output
					DataSet<T> output = runInternal(input);

					other.wrappingOperator.setInput(output);
					wrappingOperator = other.wrappingOperator;

					return wrappingOperator;
				}
			}
		}

		// no mergeable configuration found so generate new output
		DataSet<T> output = runInternal(input);

		// create a new operator to wrap the algorithm output
		wrappingOperator = new NoOpOperator<>(output, output.getType());

		// cache this result
		if (cache.containsKey(this)) {
			cache.get(this).add(this);
		} else {
			cache.put(this, new ArrayList(Collections.singletonList(this)));
		}

		return wrappingOperator;
	}
}
