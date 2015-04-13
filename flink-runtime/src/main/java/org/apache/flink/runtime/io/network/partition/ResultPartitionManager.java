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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a
 * task manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

	/**
	 * Table of running/finished ResultPartitions with corresponding IDs
	 */
	public final Table<ExecutionAttemptID, IntermediateResultPartitionID, ResultPartition>
			registeredPartitions = HashBasedTable.create();

	/**
	 * Cached ResultPartitions which are used to resume/recover from
	 */
	private final HashMap<IntermediateResultPartitionID, ResultPartition> cachedResultPartitions =
			new LinkedHashMap<IntermediateResultPartitionID, ResultPartition>();

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
			BufferProvider bufferProvider) throws IOException {

		synchronized (registeredPartitions) {
			final ResultPartition partition = registeredPartitions.get(partitionId.getProducerId(),
					partitionId.getPartitionId());

			if (partition == null) {
				throw new IOException("Unknown partition " + partitionId + ".");
			}

			LOG.debug("Requested partition {}.", partition);

			return partition.createSubpartitionView(subpartitionIndex, bufferProvider);
		}
	}

	public void releasePartitionsProducedBy(ExecutionAttemptID executionId) {
		synchronized (registeredPartitions) {
			final Map<IntermediateResultPartitionID, ResultPartition> partitions =
					registeredPartitions.row(executionId);

			for (ResultPartition partition : partitions.values()) {
				// move to cache if cachable
				updateIntermediateResultPartitionCache(partition);
			}

			for (IntermediateResultPartitionID partitionId : ImmutableList
					.copyOf(partitions.keySet())) {

				registeredPartitions.remove(executionId, partitionId);
			}

			LOG.debug("Released all partitions produced by {}.", executionId);
		}
	}

	/**
	 * Moves a produced ResultPartition to a the cache where it can be retrieved and put back
	 * to the registeredPartitions later on.
	 * @param partition
	 */
	public void updateIntermediateResultPartitionCache(ResultPartition partition) {
		synchronized (cachedResultPartitions) {
			// cache only persistent results
			if (partition.isFinished() && partition.getPartitionType().isPersistent()) {
				IntermediateResultPartitionID intermediateResultPartitionID = partition.getPartitionId().getPartitionId();
				// remove if already registered
				cachedResultPartitions.remove(intermediateResultPartitionID);
				// add as most recently inserted element
				cachedResultPartitions.put(intermediateResultPartitionID, partition);
			} else {
				partition.release();
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

	/**
	 * Registers and pins a cached ResultPartition that holds the data for an IntermediateResultPartition.
	 * @param partitionID The IntermediateResultPartitionID to find a corresponding ResultPartition for.
	 * @param numConsumers The number of consumers that want to access the ResultPartition
	 * @return true if the registering/pinning succeeded, false otherwise.
	 */
	public boolean pinCachedResultPartition(IntermediateResultPartitionID partitionID, int numConsumers) {
		synchronized (cachedResultPartitions) {
			ResultPartition resultPartition = cachedResultPartitions.get(partitionID);
			if (resultPartition != null) {
				try {
					// update its least recently used value
					updateIntermediateResultPartitionCache(resultPartition);

					synchronized (registeredPartitions) {
						if (!registeredPartitions.containsValue(resultPartition)) {
							LOG.debug("Registered previously cached ResultPartition {}.", resultPartition);
							registerResultPartition(resultPartition);
						}
					}

					for (int i = 0; i < numConsumers; i++) {
						resultPartition.pin();
					}

					LOG.debug("Pinned the ResultPartition {} for the intermediate result {}.", resultPartition, partitionID);
					return true;
				} catch (IOException e) {
					throw new IllegalStateException("Failed to pin the ResultPartition for the intermediate result partition " + partitionID);
				}
			}
			return false;
		}
	}


	// ------------------------------------------------------------------------
	// Notifications
	// ------------------------------------------------------------------------

	/**
	 * Notification from a @link{ResultPartition} when no pending references exist anymore
	 * @param partition
	 */
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
			// move to cache if cachable
			updateIntermediateResultPartitionCache(partition);

			LOG.debug("Cached {}.", partition);
		}
	}

	/**
	 * Triggered by @link{NetworkBufferPool} when network buffers should be freed
	 * @param requiredBuffers The number of buffers that should be cleared.
	 */
	public boolean releaseLeastRecentlyUsedCachedPartitions (int requiredBuffers) {
		synchronized (cachedResultPartitions) {
			// make a list of ResultPartitions to release
			List<ResultPartition> toBeReleased = new ArrayList<ResultPartition>();
			int numBuffersToBeFreed = 0;

			// traverse from least recently used cached ResultPartition
			for (Map.Entry<IntermediateResultPartitionID, ResultPartition> entry : cachedResultPartitions.entrySet()) {
				ResultPartition cachedResult = entry.getValue();

				synchronized (registeredPartitions) {
					if (!registeredPartitions.containsValue(cachedResult)) {
						if (numBuffersToBeFreed < requiredBuffers) {
							toBeReleased.add(cachedResult);
							numBuffersToBeFreed += cachedResult.getTotalNumberOfBuffers();
						}
					}
				}

				// check if we reached the desired number of buffers
				if (numBuffersToBeFreed >= requiredBuffers) {
					for (ResultPartition result : toBeReleased) {
						result.release();
						cachedResultPartitions.remove(entry.getKey());
					}
					return true;
				}
			}
			return false;
		}
	}
}
