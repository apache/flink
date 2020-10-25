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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a
 * task manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

	private final Map<ResultPartitionID, ResultPartition> registeredPartitions = new HashMap<>(16);

	private boolean isShutdown;

	public void registerResultPartition(ResultPartition partition) {
		synchronized (registeredPartitions) {
			checkState(!isShutdown, "Result partition manager already shut down.");

			ResultPartition previous = registeredPartitions.put(partition.getPartitionId(), partition);

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

		final ResultSubpartitionView subpartitionView;
		synchronized (registeredPartitions) {
			final ResultPartition partition = registeredPartitions.get(partitionId);

			if (partition == null) {
				throw new PartitionNotFoundException(partitionId);
			}

			LOG.debug("Requesting subpartition {} of {}.", subpartitionIndex, partition);

			subpartitionView = partition.createSubpartitionView(subpartitionIndex, availabilityListener);
		}

		return subpartitionView;
	}

	public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
		synchronized (registeredPartitions) {
			ResultPartition resultPartition = registeredPartitions.remove(partitionId);
			if (resultPartition != null) {
				resultPartition.release(cause);
				LOG.debug("Released partition {} produced by {}.",
					partitionId.getPartitionId(), partitionId.getProducerId());
			}
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
		LOG.debug("Received consume notification from {}.", partition);

		synchronized (registeredPartitions) {
			final ResultPartition previous = registeredPartitions.remove(partition.getPartitionId());
			// Release the partition if it was successfully removed
			if (partition == previous) {
				partition.release();
				ResultPartitionID partitionId = partition.getPartitionId();
				LOG.debug("Released partition {} produced by {}.",
					partitionId.getPartitionId(), partitionId.getProducerId());
			}
		}
	}

	public Collection<ResultPartitionID> getUnreleasedPartitions() {
		synchronized (registeredPartitions) {
			return registeredPartitions.keySet();
		}
	}
}
