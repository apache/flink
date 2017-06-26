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

package org.apache.flink.graph.library.clustering;

import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Common configuration for directed and undirected Triangle Listing algorithms.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @param <R> result type
 */
public abstract class TriangleListingBase<K, VV, EV, R>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, R> {

	// Optional configuration
	protected OptionalBoolean sortTriangleVertices = new OptionalBoolean(false, true);

	protected int littleParallelism = PARALLELISM_DEFAULT;

	/**
	 * Normalize the triangle listing such that for each result (K0, K1, K2)
	 * the vertex IDs are sorted K0 < K1 < K2.
	 *
	 * @param sortTriangleVertices whether to output each triangle's vertices in sorted order
	 * @return this
	 */
	public TriangleListingBase<K, VV, EV, R> setSortTriangleVertices(boolean sortTriangleVertices) {
		this.sortTriangleVertices.set(sortTriangleVertices);

		return this;
	}

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public TriangleListingBase<K, VV, EV, R> setLittleParallelism(int littleParallelism) {
		Preconditions.checkArgument(littleParallelism > 0 || littleParallelism == PARALLELISM_DEFAULT,
			"The parallelism must be greater than zero.");

		this.littleParallelism = littleParallelism;

		return this;
	}

	@Override
	protected final boolean mergeConfiguration(GraphAlgorithmWrappingBase other) {
		Preconditions.checkNotNull(other);

		if (!TriangleListingBase.class.isAssignableFrom(other.getClass())) {
			return false;
		}

		TriangleListingBase rhs = (TriangleListingBase) other;

		// merge configurations

		sortTriangleVertices.mergeWith(rhs.sortTriangleVertices);
		littleParallelism = (littleParallelism == PARALLELISM_DEFAULT) ? rhs.littleParallelism :
			((rhs.littleParallelism == PARALLELISM_DEFAULT) ? littleParallelism : Math.min(littleParallelism, rhs.littleParallelism));

		return true;
	}
}
