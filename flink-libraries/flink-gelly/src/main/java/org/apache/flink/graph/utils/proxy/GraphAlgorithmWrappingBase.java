/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.utils.proxy;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * A {@link GraphAlgorithm} transforms an input {@link Graph} into an output of
 * type {@code T}. Subclasses of {@link GraphAlgorithmWrappingBase} wrap the
 * output with a {@code NoOpOperator}. The input to the wrapped operators can
 * be replaced when the same algorithm is run on the same input with a
 * mergeable configuration. This allows algorithms to be composed of implicitly
 * reusable algorithms without publicly sharing intermediate {@link DataSet}s.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @param <R> result type
 */
public abstract class GraphAlgorithmWrappingBase<K, VV, EV, R>
implements GraphAlgorithm<K, VV, EV, R> {

	protected int parallelism = PARALLELISM_DEFAULT;

	/**
	 * Set the parallelism for this algorithm's operators. This parameter is
	 * necessary because processing a small amount of data with high operator
	 * parallelism is slow and wasteful with memory and buffers.
	 *
	 * <p>Operator parallelism should be set to this given value unless
	 * processing asymptotically more data, in which case the default job
	 * parallelism should be inherited.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public GraphAlgorithmWrappingBase<K, VV, EV, R> setParallelism(int parallelism) {
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT,
				"The parallelism must be at least one, or ExecutionConfig.PARALLELISM_DEFAULT (use system default).");

		this.parallelism = parallelism;

		return this;
	}

	/**
	 * Algorithms are identified by name rather than by class to allow subclassing.
	 *
	 * @return name of the algorithm, which may be shared by multiple classes
	 *		 implementing the same algorithm and generating the same output
	 */
	public String getAlgorithmName() {
		return this.getClass().getName();
	}

	/**
	 * First test whether the algorithm configurations can be merged before the
	 * call to {@link #mergeConfiguration}.
	 *
	 * @param other the algorithm with which to compare configuration
	 * @return true if and only if configuration can be merged and the
	 *         algorithm's output can be reused
	 *
	 * @see #mergeConfiguration(GraphAlgorithmWrappingBase)
	 */
	protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
		Preconditions.checkNotNull(other);

		return this.getClass().equals(other.getClass());
	}

	/**
	 * Merge the other configuration into this algorithm's after the call to
	 * {@link #canMergeConfigurationWith} has checked that the configurations
	 * can be merged.
	 *
	 * @param other the algorithm from which to merge configuration
	 *
	 * @see #canMergeConfigurationWith(GraphAlgorithmWrappingBase)
	 */
	protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
		Preconditions.checkNotNull(other);

		parallelism = (parallelism == PARALLELISM_DEFAULT) ? other.parallelism :
			((other.parallelism == PARALLELISM_DEFAULT) ? parallelism : Math.min(parallelism, other.parallelism));
	}
}
