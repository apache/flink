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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.taskmanager.TaskActions;

import static org.mockito.Mockito.mock;

/**
 * This class should consolidate all mocking logic for ResultPartitions.
 * While using Mockito internally (for now), the use of Mockito should not
 * leak out of this class.
 */
public class PartitionTestUtils {

	public static ResultPartition createMockPartition() {
		return mock(ResultPartition.class);
	}

	public static ResultPartition createPartition(IOManager ioManager, ResultPartitionType type) {
		return createPartition(
				ioManager,
				new NoOpResultPartitionConsumableNotifier(),
				type,
				false);
	}

	public static ResultPartition createPartition(
			ResultPartitionConsumableNotifier notifier,
			ResultPartitionType type,
			boolean sendScheduleOrUpdateConsumersMessage) {

		return createPartition(
				mock(IOManager.class),
				notifier,
				type,
				sendScheduleOrUpdateConsumersMessage);
	}

	public static ResultPartition createPartition(
			IOManager ioManager,
			ResultPartitionConsumableNotifier notifier,
			ResultPartitionType type,
			boolean sendScheduleOrUpdateConsumersMessage) {

		return new ResultPartition(
				"TestTask",
				mock(TaskActions.class),
				new JobID(),
				new ResultPartitionID(),
				type,
				1,
				1,
				mock(ResultPartitionManager.class),
				notifier,
				ioManager,
				sendScheduleOrUpdateConsumersMessage);
	}
}
