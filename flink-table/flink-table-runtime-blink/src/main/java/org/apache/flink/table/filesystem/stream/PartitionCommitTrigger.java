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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_TRIGGER;

/**
 * Abstract commit trigger, store partitions in state and provide {@link #committablePartitions}
 * for trigger.
 * See {@link PartitionTimeCommitTigger}.
 * See {@link ProcTimeCommitTigger}.
 */
public abstract class PartitionCommitTrigger {

	public static final String PARTITION_TIME = "partition-time";
	public static final String PROCESS_TIME = "process-time";

	private static final ListStateDescriptor<List<String>> PENDING_PARTITIONS_STATE_DESC =
			new ListStateDescriptor<>(
					"pending-partitions",
					new ListSerializer<>(StringSerializer.INSTANCE));

	protected final ListState<List<String>> pendingPartitionsState;
	protected final Set<String> pendingPartitions;

	protected PartitionCommitTrigger(
			boolean isRestored, OperatorStateStore stateStore) throws Exception {
		this.pendingPartitionsState = stateStore.getListState(PENDING_PARTITIONS_STATE_DESC);
		this.pendingPartitions = new HashSet<>();
		if (isRestored) {
			pendingPartitions.addAll(pendingPartitionsState.get().iterator().next());
		}
	}

	/**
	 * Add a pending partition.
	 */
	public void addPartition(String partition) {
		if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
			this.pendingPartitions.add(partition);
		}
	}

	/**
	 * Get committable partitions, and cleanup useless watermarks and partitions.
	 */
	public abstract List<String> committablePartitions(long checkpointId) throws IOException;

	/**
	 * End input, return committable partitions and clear.
	 */
	public List<String> endInput() {
		ArrayList<String> partitions = new ArrayList<>(pendingPartitions);
		pendingPartitions.clear();
		return partitions;
	}

	/**
	 * Snapshot state.
	 */
	public void snapshotState(long checkpointId, long watermark) throws Exception {
		pendingPartitionsState.clear();
		pendingPartitionsState.add(new ArrayList<>(pendingPartitions));
	}

	public static PartitionCommitTrigger create(
			boolean isRestored,
			OperatorStateStore stateStore,
			Configuration conf,
			ClassLoader cl,
			List<String> partitionKeys,
			ProcessingTimeService procTimeService,
			FileSystem fileSystem,
			Path locationPath) throws Exception {
		String trigger = conf.get(SINK_PARTITION_COMMIT_TRIGGER);
		switch (trigger) {
			case PARTITION_TIME:
				return new PartitionTimeCommitTigger(
						isRestored, stateStore, conf, cl, partitionKeys);
			case PROCESS_TIME:
				return new ProcTimeCommitTigger(
						isRestored, stateStore, conf, procTimeService, fileSystem, locationPath);
			default:
				throw new UnsupportedOperationException(
						"Unsupported partition commit trigger: " + trigger);
		}
	}
}
