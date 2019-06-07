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

import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.function.FunctionWithException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * ResultPartition that releases itself once all subpartitions have been consumed.
 */
public class ReleaseOnConsumptionResultPartition extends ResultPartition {

	/**
	 * The total number of references to subpartitions of this result. The result partition can be
	 * safely released, iff the reference count is zero. A reference count of -1 denotes that the
	 * result partition has been released.
	 */
	private final AtomicInteger pendingReferences = new AtomicInteger();

	ReleaseOnConsumptionResultPartition(
			String owningTaskName,
			ResultPartitionID partitionId,
			ResultPartitionType partitionType,
			ResultSubpartition[] subpartitions,
			int numTargetKeyGroups,
			ResultPartitionManager partitionManager,
			FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {
		super(owningTaskName, partitionId, partitionType, subpartitions, numTargetKeyGroups, partitionManager, bufferPoolFactory);
	}

	/**
	 * The partition can only be released after each subpartition has been consumed once per pin operation.
	 */
	@Override
	void pin() {
		while (true) {
			int refCnt = pendingReferences.get();

			if (refCnt >= 0) {
				if (pendingReferences.compareAndSet(refCnt, refCnt + subpartitions.length)) {
					break;
				}
			}
			else {
				throw new IllegalStateException("Released.");
			}
		}
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		int refCnt = pendingReferences.get();

		checkState(refCnt != -1, "Partition released.");
		checkState(refCnt > 0, "Partition not pinned.");

		return super.createSubpartitionView(index, availabilityListener);
	}

	@Override
	void onConsumedSubpartition(int subpartitionIndex) {
		if (isReleased()) {
			return;
		}

		int refCnt = pendingReferences.decrementAndGet();

		if (refCnt == 0) {
			partitionManager.onConsumedPartition(this);
		} else if (refCnt < 0) {
			throw new IllegalStateException("All references released.");
		}

		LOG.debug("{}: Received release notification for subpartition {}.",
			this, subpartitionIndex);
	}

	@Override
	public String toString() {
		return "ReleaseOnConsumptionResultPartition " + partitionId.toString() + " [" + partitionType + ", "
			+ subpartitions.length + " subpartitions, "
			+ pendingReferences + " pending references]";
	}
}
