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
	protected boolean permuteResults;

	protected OptionalBoolean sortTriangleVertices = new OptionalBoolean(false, true);

	/**
	 * By default only one result is output for each triangle, whether vertices
	 * are sorted or unsorted. When permutation is enabled a result is instead
	 * output for each of the six permutations of the three vertex IDs.
	 *
	 * @param permuteResults whether output results should be permuted
	 * @return this
	 */
	public TriangleListingBase<K, VV, EV, R> setPermuteResults(boolean permuteResults) {
		this.permuteResults = permuteResults;

		return this;
	}

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

	@Override
	protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
		if (!super.canMergeConfigurationWith(other)) {
			return false;
		}

		TriangleListingBase rhs = (TriangleListingBase) other;

		return permuteResults == rhs.permuteResults;
	}

	@Override
	protected final void mergeConfiguration(GraphAlgorithmWrappingBase other) {
		super.mergeConfiguration(other);

		TriangleListingBase rhs = (TriangleListingBase) other;

		sortTriangleVertices.mergeWith(rhs.sortTriangleVertices);
	}
}
