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
	 * An algorithm must first test whether the configurations can be merged
	 * before merging individual fields.
	 *
	 * @param other the algorithm with which to compare and merge
	 * @return true if and only if configuration has been merged and the
	 *          algorithm's output can be reused
	 */
	protected abstract boolean mergeConfiguration(GraphAlgorithmWrappingBase<K, VV, EV, R> other);
}
