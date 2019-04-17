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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.disk.iomanager.NoOpIOManager;
import org.apache.flink.runtime.taskmanager.NoOpTaskActions;

/**
 * This class should consolidate all mocking logic for ResultPartitions.
 * While using Mockito internally (for now), the use of Mockito should not
 * leak out of this class.
 */
public class PartitionTestUtils {

	public static ResultPartition createPartition() {
		return createPartition(ResultPartitionType.PIPELINED_BOUNDED);
	}

	public static ResultPartition createPartition(ResultPartitionType type) {
		return createPartition(
				new NoOpResultPartitionConsumableNotifier(),
				type,
				false);
	}

	public static ResultPartition createPartition(ResultPartitionType type, int numChannels) {
		return createPartition(new NoOpResultPartitionConsumableNotifier(), type, numChannels, false);
	}

	public static ResultPartition createPartition(
			ResultPartitionConsumableNotifier notifier,
			ResultPartitionType type,
			boolean sendScheduleOrUpdateConsumersMessage) {

		return createPartition(notifier, type, 1, sendScheduleOrUpdateConsumersMessage);
	}

	public static ResultPartition createPartition(
			ResultPartitionConsumableNotifier notifier,
			ResultPartitionType type,
			int numChannels,
			boolean sendScheduleOrUpdateConsumersMessage) {

		return new ResultPartition(
				"TestTask",
				new NoOpTaskActions(),
				new JobID(),
				new ResultPartitionID(),
				type,
				numChannels,
				numChannels,
				new ResultPartitionManager(),
				notifier,
				new NoOpIOManager(),
				sendScheduleOrUpdateConsumersMessage);
	}
}
