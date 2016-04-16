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

package org.apache.flink.graph;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.DataSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link GraphAlgorithm} transforms an input {@link Graph} into an output of
 * type {@code T}. A {@code CachingGraphAlgorithm} stores the mapping of inputs
 * to outputs and will return the same output object for the same input object
 * and configuration. This allows algorithms to be composed of reusable
 * transformations without exposing intermediate {@link DataSet}s.
 *
 * @param <K> graph label type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @param <T> output type
 */
public abstract class CachingGraphAlgorithm<K, VV, EV, T>
implements GraphAlgorithm<K, VV, EV, T> {

	// each CachingGraphAlgorithm object stores its input, output, and configuration
	private static Map<CachingGraphAlgorithm,CachingGraphAlgorithm> cache =
		Collections.synchronizedMap(new HashMap<CachingGraphAlgorithm,CachingGraphAlgorithm>());

	private Graph<K,VV,EV> input;

	private T output;

	/**
	 * {@code hashCodeInternal} follows the same contract for {@code equalsInternal}
	 * as {@link Object#hashCode} for {@link Object#equals(Object)}.
	 *
	 * @param builder HashCodeBuilder for algorithm configuration
	 */
	protected void hashCodeInternal(HashCodeBuilder builder) {};

	/**
	 * Any configuration fields which affect the output must be checked for
	 * equality. For fields such as {@code parallelism} which affect how the
	 * output is processed but do not change the resulting output, the algorithm
	 * may choose whether to include in the test for algorithm equality.
	 * <br/>
	 * {@code equalsInternal} follows the same contract for {@code hashCodeInternal}
	 * as {@link Object#equals(Object)} for {@link Object#hashCode}.
	 *
	 * @param builder EqualsBuilder for algorithm configuration
	 * @param obj the reference object with which to compare
	 */
	protected void equalsInternal(EqualsBuilder builder, CachingGraphAlgorithm obj) {};

	/**
	 * Algorithms are identified by name rather than by class to allow subclassing.
	 *
	 * @return name of the algorithm, which may be shared by multiple classes
	 *         implementing the same algorithm and generating the same output
	 */
	protected abstract String getAlgorithmName();

	/**
	 * The implementation of the algorithm, renamed from {@link GraphAlgorithm#run(Graph)}.
	 *
	 * @param input the input graph
	 * @return the algorithm's output
	 * @throws Exception
	 */
	protected abstract T runInternal(Graph<K, VV, EV> input) throws Exception;

	@Override
	public final int hashCode() {
		HashCodeBuilder builder = new HashCodeBuilder(17, 37)
			.append(input);

		builder.append(getAlgorithmName());

		hashCodeInternal(builder);

		return builder.toHashCode();
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		if (! CachingGraphAlgorithm.class.isAssignableFrom(obj.getClass())) {
			return false;
		}

		CachingGraphAlgorithm rhs = (CachingGraphAlgorithm) obj;

		EqualsBuilder builder = new EqualsBuilder()
			.append(getAlgorithmName(), rhs.getAlgorithmName())
			.append(input, rhs.input);

		equalsInternal(builder, rhs);

		return builder.isEquals();
	}

	@Override
	@SuppressWarnings("unchecked")
	public final T run(Graph<K, VV, EV> input)
			throws Exception {
		this.input = input;

		if (cache.containsKey(this)) {
			return (T) cache.get(this).output;
		}

		output = runInternal(input);

		cache.put(this, this);

		return output;
	}
}
