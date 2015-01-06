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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.jobgraph.ResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a
 * task manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

	public final Table<ExecutionAttemptID, ResultPartitionID, ResultPartition> partitions = HashBasedTable.create();

	private boolean isShutdown;

	public void registerIntermediateResultPartition(ResultPartition partition) throws IOException {
		synchronized (partitions) {
			checkState(!isShutdown, "Result partition manager already shut down.");

			ResultPartition previous = partitions.put(partition.getExecutionId(), partition.getPartitionId(), partition);

			if (previous != null) {
				throw new IllegalStateException("Result partition already registered.");
			}
		}
	}

	@Override
	public ResultSubpartitionView getSubpartition(ExecutionAttemptID executionId, ResultPartitionID partitionId, int subpartitionIndex, Optional<BufferProvider> bufferProvider) throws IOException {
		synchronized (partitions) {
			ResultPartition partition = partitions.get(executionId, partitionId);

			if (partition == null) {
				throw new IOException(String.format("Unknown partition %s:%s.", executionId, partitionId));
			}

			return partition.getSubpartition(subpartitionIndex, bufferProvider);
		}
	}

	public void releasePartitionsProducedBy(ExecutionAttemptID executionId) {
		synchronized (partitions) {
			Map<ResultPartitionID, ResultPartition> row = partitions.row(executionId);

			for (ResultPartition partition : partitions.row(executionId).values()) {
				partition.unpin();

				partition.release();
			}

			for (ResultPartitionID partitionId : ImmutableList.copyOf(row.keySet())) {
				partitions.remove(executionId, partitionId);
			}
		}
	}

	public void shutdown() {
		synchronized (partitions) {
			for (ResultPartition partition : partitions.values()) {
				partition.release();
			}

			partitions.clear();

			isShutdown = true;
		}
	}

	// ------------------------------------------------------------------------
	// Notifications
	// ------------------------------------------------------------------------

	void onConsumedPartition(ResultPartition partition) {
		synchronized (partitions) {
			// TODO Add historic intermediate result manager, where this partition can live on
			partitions.remove(partition.getExecutionId(), partition.getPartitionId());

			partition.release();
		}
	}
}
