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

import org.apache.flink.runtime.io.network.NetworkEnvironment;

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
		return new ResultPartitionBuilder().setResultPartitionType(type).build();
	}

	public static ResultPartition createPartition(
			ResultPartitionConsumableNotifier notifier,
			ResultPartitionType type,
			boolean sendScheduleOrUpdateConsumersMessage) {
		return new ResultPartitionBuilder()
			.setResultPartitionConsumableNotifier(notifier)
			.setResultPartitionType(type)
			.setSendScheduleOrUpdateConsumersMessage(sendScheduleOrUpdateConsumersMessage)
			.build();
	}

	public static ResultPartition createPartition(
			NetworkEnvironment environment,
			ResultPartitionType partitionType,
			int numChannels) {
		return new ResultPartitionBuilder()
			.setupBufferPoolFactoryFromNetworkEnvironment(environment)
			.setResultPartitionType(partitionType)
			.setNumberOfSubpartitions(numChannels)
			.build();
	}
}
