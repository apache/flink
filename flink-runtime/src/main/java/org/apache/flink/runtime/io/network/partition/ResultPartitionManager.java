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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.apache.flink.shaded.guava18.com.google.common.collect.HashBasedTable;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a
 * task manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

	public final Table<ExecutionAttemptID, IntermediateResultPartitionID, ResultPartition>
			registeredPartitions = HashBasedTable.create();

	private boolean isShutdown;

	public void registerResultPartition(ResultPartition partition) throws IOException {
		synchronized (registeredPartitions) {
			checkState(!isShutdown, "Result partition manager already shut down.");

			ResultPartitionID partitionId = partition.getPartitionId();

			ResultPartition previous = registeredPartitions.put(
					partitionId.getProducerId(), partitionId.getPartitionId(), partition);

			if (previous != null) {
				throw new IllegalStateException("Result partition already registered.");
			}

			LOG.debug("Registered {}.", partition);
		}
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			ResultPartitionID partitionId,
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {

		synchronized (registeredPartitions) {
			final ResultPartition partition = registeredPartitions.get(partitionId.getProducerId(),
					partitionId.getPartitionId());

			if (partition == null) {
				throw new PartitionNotFoundException(partitionId);
			}

			LOG.debug("Requesting subpartition {} of {}.", subpartitionIndex, partition);

			return partition.createSubpartitionView(subpartitionIndex, availabilityListener);
		}
	}

	public void releasePartitionsProducedBy(ExecutionAttemptID executionId) {
		releasePartitionsProducedBy(executionId, null);
	}

	public void releasePartitionsProducedBy(ExecutionAttemptID executionId, Throwable cause) {
		synchronized (registeredPartitions) {
			final Map<IntermediateResultPartitionID, ResultPartition> partitions =
					registeredPartitions.row(executionId);

			for (ResultPartition partition : partitions.values()) {
				partition.release(cause);
			}

			for (IntermediateResultPartitionID partitionId : ImmutableList
					.copyOf(partitions.keySet())) {

				registeredPartitions.remove(executionId, partitionId);
			}

			LOG.debug("Released all partitions produced by {}.", executionId);
		}
	}

	public void shutdown() {
		synchronized (registeredPartitions) {

			LOG.debug("Releasing {} partitions because of shutdown.",
					registeredPartitions.values().size());

			for (ResultPartition partition : registeredPartitions.values()) {
				partition.release();
			}

			registeredPartitions.clear();

			isShutdown = true;

			LOG.debug("Successful shutdown.");
		}
	}

	// ------------------------------------------------------------------------
	// Notifications
	// ------------------------------------------------------------------------

	void onConsumedPartition(ResultPartition partition) {
		final ResultPartition previous;

		LOG.debug("Received consume notification from {}.", partition);

		synchronized (registeredPartitions) {
			ResultPartitionID partitionId = partition.getPartitionId();

			previous = registeredPartitions.remove(partitionId.getProducerId(),
					partitionId.getPartitionId());
		}

		// Release the partition if it was successfully removed
		if (partition == previous) {
			partition.release();

			LOG.debug("Released {}.", partition);
		}
	}
}
