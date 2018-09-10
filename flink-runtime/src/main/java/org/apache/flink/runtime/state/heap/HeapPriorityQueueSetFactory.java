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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Factory for {@link HeapPriorityQueueSet}.
 */
public class HeapPriorityQueueSetFactory implements PriorityQueueSetFactory {

	@Nonnull
	private final KeyGroupRange keyGroupRange;

	@Nonnegative
	private final int totalKeyGroups;

	@Nonnegative
	private final int minimumCapacity;

	public HeapPriorityQueueSetFactory(
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalKeyGroups,
		@Nonnegative int minimumCapacity) {

		this.keyGroupRange = keyGroupRange;
		this.totalKeyGroups = totalKeyGroups;
		this.minimumCapacity = minimumCapacity;
	}

	@Nonnull
	@Override
	public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> HeapPriorityQueueSet<T> create(
		@Nonnull String stateName,
		@Nonnull TypeSerializer<T> byteOrderedElementSerializer) {

		return new HeapPriorityQueueSet<>(
			PriorityComparator.forPriorityComparableObjects(),
			KeyExtractorFunction.forKeyedObjects(),
			minimumCapacity,
			keyGroupRange,
			totalKeyGroups);
	}
}
