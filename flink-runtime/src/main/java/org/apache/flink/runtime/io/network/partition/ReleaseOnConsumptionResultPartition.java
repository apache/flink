/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * ResultPartition that releases itself once all subpartitions have been consumed.
 */
public class ReleaseOnConsumptionResultPartition extends ResultPartition {

	private static final Object lock = new Object();

	/**
	 * A flag for each subpartition indicating whether it was already consumed or not.
	 */
	private final boolean[] consumedSubpartitions;

	/**
	 * The total number of references to subpartitions of this result. The result partition can be
	 * safely released, iff the reference count is zero.
	 */
	private int numUnconsumedSubpartitions;

	ReleaseOnConsumptionResultPartition(
			String owningTaskName,
			ResultPartitionID partitionId,
			ResultPartitionType partitionType,
			ResultSubpartition[] subpartitions,
			int numTargetKeyGroups,
			ResultPartitionManager partitionManager,
			@Nullable BufferCompressor bufferCompressor,
			FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {
		super(
			owningTaskName,
			partitionId,
			partitionType,
			subpartitions,
			numTargetKeyGroups,
			partitionManager,
			bufferCompressor,
			bufferPoolFactory);

		this.consumedSubpartitions = new boolean[subpartitions.length];
		this.numUnconsumedSubpartitions = subpartitions.length;
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		checkState(numUnconsumedSubpartitions > 0, "Partition not pinned.");

		return super.createSubpartitionView(index, availabilityListener);
	}

	@Override
	void onConsumedSubpartition(int subpartitionIndex) {
		if (isReleased()) {
			return;
		}

		final int remainingUnconsumed;

		// we synchronize only the bookkeeping section, to avoid holding the lock during any
		// calls into other components
		synchronized (lock) {
			if (consumedSubpartitions[subpartitionIndex]) {
				// repeated call - ignore
				return;
			}

			consumedSubpartitions[subpartitionIndex] = true;
			remainingUnconsumed = (--numUnconsumedSubpartitions);
		}

		LOG.debug("{}: Received consumed notification for subpartition {}.", this, subpartitionIndex);

		if (remainingUnconsumed == 0) {
			partitionManager.onConsumedPartition(this);
		} else if (remainingUnconsumed < 0) {
			throw new IllegalStateException("Received consume notification even though all subpartitions are already consumed.");
		}
	}

	@Override
	public String toString() {
		return "ReleaseOnConsumptionResultPartition " + partitionId.toString() + " [" + partitionType + ", "
			+ subpartitions.length + " subpartitions, "
			+ numUnconsumedSubpartitions + " pending consumptions]";
	}
}
