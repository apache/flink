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

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_TRIGGER;

/**
 * Partition commit trigger.
 * See {@link PartitionTimeCommitTrigger}.
 * See {@link ProcTimeCommitTrigger}.
 */
public interface PartitionCommitTrigger {

	String PARTITION_TIME = "partition-time";
	String PROCESS_TIME = "process-time";

	/**
	 * Add a pending partition.
	 */
	void addPartition(String partition);

	/**
	 * Get committable partitions, and cleanup useless watermarks and partitions.
	 */
	List<String> committablePartitions(long checkpointId) throws IOException;

	/**
	 * End input, return committable partitions and clear.
	 */
	List<String> endInput();

	/**
	 * Snapshot state.
	 */
	void snapshotState(long checkpointId, long watermark) throws Exception;

	static PartitionCommitTrigger create(
			boolean isRestored,
			OperatorStateStore stateStore,
			Configuration conf,
			ClassLoader cl,
			List<String> partitionKeys,
			ProcessingTimeService procTimeService) throws Exception {
		String trigger = conf.get(SINK_PARTITION_COMMIT_TRIGGER);
		switch (trigger) {
			case PARTITION_TIME:
				return new PartitionTimeCommitTrigger(
						isRestored, stateStore, conf, cl, partitionKeys);
			case PROCESS_TIME:
				return new ProcTimeCommitTrigger(
						isRestored, stateStore, conf, procTimeService);
			default:
				throw new UnsupportedOperationException(
						"Unsupported partition commit trigger: " + trigger);
		}
	}
}
