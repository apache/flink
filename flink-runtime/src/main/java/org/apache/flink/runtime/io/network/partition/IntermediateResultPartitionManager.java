/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import com.google.common.base.Optional;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class IntermediateResultPartitionManager implements IntermediateResultPartitionProvider {

	private final ConcurrentMap<IntermediateResultPartitionID, IntermediateResultPartition> partitions =
			new ConcurrentHashMap<IntermediateResultPartitionID, IntermediateResultPartition>();

	private final AtomicBoolean isShutdown = new AtomicBoolean(false);

	public void registerIntermediateResultPartition(IntermediateResultPartition partition) {
		if (!isShutdown.get()) {
			partitions.putIfAbsent(partition.getPartitionId(), partition);
		}
	}

	public void unregisterIntermediateResultPartition(IntermediateResultPartition partition) {
		partitions.remove(partition.getPartitionId());
	}

	public void shutdown() {
		if (isShutdown.compareAndSet(false, true)) {
			// discard all partitions
		}
	}

	// ------------------------------------------------------------------------
	// Intermediate result partition provider
	// ------------------------------------------------------------------------

	@Override
	public IntermediateResultPartitionQueueIterator getIntermediateResultPartitionIterator(
			IntermediateResultPartitionID partitionId,
			int requestedQueueIndex,
			Optional<BufferProvider> bufferProvider) {

		return partitions.get(partitionId).getQueueIterator(requestedQueueIndex, bufferProvider);
	}
}
