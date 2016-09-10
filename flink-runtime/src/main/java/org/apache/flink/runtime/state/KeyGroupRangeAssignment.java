/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

public final class KeyGroupRangeAssignment {

	public static final int DEFAULT_MAX_PARALLELISM = 128;

	private KeyGroupRangeAssignment() {
		throw new AssertionError();
	}

	/**
	 * Assigns the given key to a parallel operator index.
	 *
	 * @param key the key to assign
	 * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
	 * @param parallelism the current parallelism of the operator
	 * @return the index of the parallel operator to which the given key should be routed.
	 */
	public static int assignKeyToParallelOperator(Object key, int maxParallelism, int parallelism) {
		return computeOperatorIndexForKeyGroup(maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));
	}

	/**
	 * Assigns the given key to a key-group index.
	 *
	 * @param key the key to assign
	 * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
	 * @return the key-group to which the given key is assigned
	 */
	public static final int assignToKeyGroup(Object key, int maxParallelism) {
		return MathUtils.murmurHash(key.hashCode()) % maxParallelism;
	}

	/**
	 * Computes the range of key-groups that are assigned to a given operator under the given parallelism and maximum
	 * parallelism.
	 *
	 * IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want
	 * to go beyond this boundary, this method must perform arithmetic on long values.
	 *
	 * @param maxParallelism Maximal parallelism that the job was initially created with.
	 * @param parallelism    The current parallelism under which the job runs. Must be <= maxParallelism.
	 * @param operatorIndex  Id of a key-group. 0 <= keyGroupID < maxParallelism.
	 * @return
	 */
	public static KeyGroupRange computeKeyGroupRangeForOperatorIndex(
			int maxParallelism,
			int parallelism,
			int operatorIndex) {
		Preconditions.checkArgument(parallelism > 0, "Parallelism must not be smaller than zero.");
		Preconditions.checkArgument(maxParallelism >= parallelism, "Maximum parallelism must not be smaller than parallelism.");
		Preconditions.checkArgument(maxParallelism <= (1 << 15), "Maximum parallelism must be smaller than 2^15.");

		int start = operatorIndex == 0 ? 0 : ((operatorIndex * maxParallelism - 1) / parallelism) + 1;
		int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
		return new KeyGroupRange(start, end);
	}

	/**
	 * Computes the index of the operator to which a key-group belongs under the given parallelism and maximum
	 * parallelism.
	 *
	 * IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want
	 * to go beyond this boundary, this method must perform arithmetic on long values.
	 *
	 * @param maxParallelism Maximal parallelism that the job was initially created with.
	 *                       0 < parallelism <= maxParallelism <= Short.MAX_VALUE must hold.
	 * @param parallelism    The current parallelism under which the job runs. Must be <= maxParallelism.
	 * @param keyGroupId     Id of a key-group. 0 <= keyGroupID < maxParallelism.
	 * @return The index of the operator to which elements from the given key-group should be routed under the given
	 * parallelism and maxParallelism.
	 */
	public static int computeOperatorIndexForKeyGroup(int maxParallelism, int parallelism, int keyGroupId) {
		return keyGroupId * parallelism / maxParallelism;
	}
}
