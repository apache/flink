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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.state.AbstractKeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupPartitionedSnapshotImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class represents the snapshot of a {@link InternalTimerHeap}.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 */
public class InternalTimerHeapSnapshot<K, N> implements StateSnapshot {

	/** Copy of the heap array containing all the (immutable timers). */
	@Nonnull
	private final TimerHeapInternalTimer<K, N>[] timerHeapArrayCopy;

	/** The timer serializer. */
	@Nonnull
	private final TimerHeapInternalTimer.TimerSerializer<K, N> timerSerializer;

	/** The key-group range covered by this snapshot. */
	@Nonnull
	private final KeyGroupRange keyGroupRange;

	/** The total number of key-groups in the job. */
	@Nonnegative
	private final int totalKeyGroups;

	/** Result of partitioning the snapshot by key-group. */
	@Nullable
	private KeyGroupPartitionedSnapshot partitionedSnapshot;

	InternalTimerHeapSnapshot(
		@Nonnull TimerHeapInternalTimer<K, N>[] timerHeapArrayCopy,
		@Nonnull TimerHeapInternalTimer.TimerSerializer<K, N> timerSerializer,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalKeyGroups) {

		this.timerHeapArrayCopy = timerHeapArrayCopy;
		this.timerSerializer = timerSerializer;
		this.keyGroupRange = keyGroupRange;
		this.totalKeyGroups = totalKeyGroups;
	}

	@Nonnull
	@Override
	public KeyGroupPartitionedSnapshot partitionByKeyGroup() {

		if (partitionedSnapshot == null) {
			TimerPartitioner<K, N> timerPartitioner =
				new TimerPartitioner<>(timerHeapArrayCopy, keyGroupRange, totalKeyGroups);
			partitionedSnapshot = new KeyGroupPartitionedSnapshotImpl<>(
				timerPartitioner.partitionByKeyGroup(),
				timerSerializer::serialize);
		}

		return partitionedSnapshot;
	}

	@Override
	public void release() {
	}

	/**
	 * Implementation of {@link AbstractKeyGroupPartitioner} for {@link TimerHeapInternalTimer}.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 */
	static final class TimerPartitioner<K, N> extends AbstractKeyGroupPartitioner<TimerHeapInternalTimer<K, N>> {

		@Nonnull
		private final TimerHeapInternalTimer<K, N>[] timersIn;

		@Nonnull
		private final TimerHeapInternalTimer<K, N>[] timersOut;

		@SuppressWarnings("unchecked")
		TimerPartitioner(
			@Nonnull TimerHeapInternalTimer<K, N>[] timers,
			@Nonnull KeyGroupRange keyGroupRange,
			@Nonnegative int totalKeyGroups) {
			super(timers.length, keyGroupRange, totalKeyGroups);
			this.timersIn = timers;
			this.timersOut = new TimerHeapInternalTimer[timers.length];
		}

		@Nonnull
		@Override
		protected Object extractKeyFromElement(TimerHeapInternalTimer<K, N> element) {
			return element.getKey();
		}

		@Nonnull
		@Override
		protected TimerHeapInternalTimer<K, N>[] getPartitioningInput() {
			return timersIn;
		}

		@Nonnull
		@Override
		protected TimerHeapInternalTimer<K, N>[] getPartitioningOutput() {
			return timersOut;
		}
	}
}
