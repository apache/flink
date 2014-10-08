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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.queue.IllegalQueueIteratorRequestException;
import org.apache.flink.runtime.io.network.partition.queue.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The intermediate result partition manager keeps track of all available
 * partitions of a task manager and
 */
public class IntermediateResultPartitionManager implements IntermediateResultPartitionProvider {

	private static final Logger LOG = LoggerFactory.getLogger(IntermediateResultPartitionManager.class);

	public final Table<ExecutionAttemptID, IntermediateResultPartitionID, IntermediateResultPartition> partitions = HashBasedTable.create();

	private boolean isShutdown;

	public void registerIntermediateResultPartition(IntermediateResultPartition partition) throws IOException {
		synchronized (partitions) {
			if (isShutdown) {
				throw new IOException("Intermediate result partition manager has already been shut down.");
			}

			if (partitions.put(partition.getProducerExecutionId(), partition.getPartitionId(), partition) != null) {
				throw new IOException("Tried to re-register intermediate result partition.");
			}
		}
	}

	public void failIntermediateResultPartitions(ExecutionAttemptID producerExecutionId) {
		synchronized (partitions) {
			List<IntermediateResultPartition> partitionsToFail = new ArrayList<IntermediateResultPartition>();

			for (IntermediateResultPartitionID partitionId : partitions.row(producerExecutionId).keySet()) {
				partitionsToFail.add(partitions.get(producerExecutionId, partitionId));
			}

			for(IntermediateResultPartition partition : partitionsToFail) {
				failIntermediateResultPartition(partition);
			}
		}
	}

	private void failIntermediateResultPartition(IntermediateResultPartition partition) {
		if (partition != null) {
			try {
				partition.releaseAllResources();
			}
			catch (Throwable t) {
				LOG.error("Error during release of produced intermediate result partition: " + t.getMessage(), t);
			}
		}
	}

	public void shutdown() {
		synchronized (partitions) {
			for (IntermediateResultPartition partition : partitions.values()) {
				try {
					partition.releaseAllResources();
				}
				catch (IOException e) {
					LOG.error("Error while releasing intermediate result partition: " + e.getMessage(), e);
				}
			}

			isShutdown = true;
		}
	}

	public int getNumberOfRegisteredPartitions() {
		synchronized (partitions) {
			return partitions.size();
		}
	}

	// ------------------------------------------------------------------------
	// Intermediate result partition provider
	// ------------------------------------------------------------------------

	@Override
	public IntermediateResultPartitionQueueIterator getIntermediateResultPartitionIterator(
			ExecutionAttemptID producerExecutionId,
			IntermediateResultPartitionID partitionId,
			int queueIndex,
			Optional<BufferProvider> bufferProvider) throws IOException {

		synchronized (partitions) {
			IntermediateResultPartition partition = partitions.get(producerExecutionId, partitionId);

			if (partition == null) {
				if (!partitions.containsRow(producerExecutionId)) {
					throw new IllegalQueueIteratorRequestException("Unknown producer execution ID " + producerExecutionId + ".");
				}

				throw new IllegalQueueIteratorRequestException("Unknown partition " + partitionId + ".");
			}

			return partition.getQueueIterator(queueIndex, bufferProvider);
		}
	}
}
