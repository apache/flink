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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Iterator;

/**
 * This class defines a range of key-group indexes. Key-groups are the granularity into which the keyspace of a job
 * is partitioned for keyed state-handling in state backends. The boundaries of the range are inclusive.
 */
public class KeyGroupRange implements Iterable<Integer>, Serializable {

	/** The empty key-group */
	public static final KeyGroupRange EMPTY_KEY_GROUP = new KeyGroupRange();

	private final int startKeyGroup;
	private final int endKeyGroup;

	/**
	 * Empty KeyGroup Constructor
	 */
	private KeyGroupRange() {
		this.startKeyGroup = 0;
		this.endKeyGroup = -1;
	}

	/**
	 * Defines the range [startKeyGroup, endKeyGroup]
	 *
	 * @param startKeyGroup start of the range (inclusive)
	 * @param endKeyGroup end of the range (inclusive)
	 */
	public KeyGroupRange(int startKeyGroup, int endKeyGroup) {
		Preconditions.checkArgument(startKeyGroup >= 0);
		Preconditions.checkArgument(startKeyGroup <= endKeyGroup);
		this.startKeyGroup = startKeyGroup;
		this.endKeyGroup = endKeyGroup;
		Preconditions.checkArgument(getNumberOfKeyGroups() >= 0, "Potential overflow detected.");
	}


	/**
	 * Checks whether or not a single key-group is contained in the range.
	 *
	 * @param keyGroup Key-group to check for inclusion.
	 * @return True, only if the key-group is in the range.
	 */
	public boolean contains(int keyGroup) {
		return keyGroup >= startKeyGroup && keyGroup <= endKeyGroup;
	}

	/**
	 * Create a range that represent the intersection between this range and the given range.
	 *
	 * @param other A KeyGroupRange to intersect.
	 * @return Key-group range that is the intersection between this and the given key-group range.
	 */
	public KeyGroupRange getIntersection(KeyGroupRange other) {
		int start = Math.max(startKeyGroup, other.startKeyGroup);
		int end = Math.min(endKeyGroup, other.endKeyGroup);
		return start <= end ? new KeyGroupRange(start, end) : EMPTY_KEY_GROUP;
	}

	/**
	 *
	 * @return The number of key-groups in the range
	 */
	public int getNumberOfKeyGroups() {
		return 1 + endKeyGroup - startKeyGroup;
	}

	/**
	 *
	 * @return The first key-group in the range.
	 */
	public int getStartKeyGroup() {
		return startKeyGroup;
	}

	/**
	 *
	 * @return The last key-group in the range.
	 */
	public int getEndKeyGroup() {
		return endKeyGroup;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof KeyGroupRange)) {
			return false;
		}

		KeyGroupRange that = (KeyGroupRange) o;
		return startKeyGroup == that.startKeyGroup && endKeyGroup == that.endKeyGroup;
	}


	@Override
	public int hashCode() {
		int result = startKeyGroup;
		result = 31 * result + endKeyGroup;
		return result;
	}

	@Override
	public String toString() {
		return "KeyGroupRange{" +
				"startKeyGroup=" + startKeyGroup +
				", endKeyGroup=" + endKeyGroup +
				'}';
	}

	@Override
	public Iterator<Integer> iterator() {
		return new KeyGroupIterator();
	}

	private final class KeyGroupIterator implements Iterator<Integer> {

		public KeyGroupIterator() {
			this.iteratorPos = 0;
		}

		private int iteratorPos;

		@Override
		public boolean hasNext() {
			return iteratorPos < getNumberOfKeyGroups();
		}

		@Override
		public Integer next() {
			int rv = startKeyGroup + iteratorPos;
			++iteratorPos;
			return rv;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Unsupported by this iterator!");
		}
	}

	/**
	 * Factory method that also handles creation of empty key-groups.
	 *
	 * @param startKeyGroup start of the range (inclusive)
	 * @param endKeyGroup end of the range (inclusive)
	 * @return the key-group from start to end or an empty key-group range.
	 */
	public static KeyGroupRange of(int startKeyGroup, int endKeyGroup) {
		return startKeyGroup <= endKeyGroup ? new KeyGroupRange(startKeyGroup, endKeyGroup) : EMPTY_KEY_GROUP;
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
		Preconditions.checkArgument(maxParallelism <= Short.MAX_VALUE, "Maximum parallelism must be smaller than Short.MAX_VALUE.");

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
	public static final int computeOperatorIndexForKeyGroup(int maxParallelism, int parallelism, int keyGroupId) {
		return keyGroupId * parallelism / maxParallelism;
	}
}
