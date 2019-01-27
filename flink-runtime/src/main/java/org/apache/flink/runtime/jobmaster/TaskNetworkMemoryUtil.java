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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.MathUtils;

/**
 * A util to calculate the network memory used for netty of a task.
 */
public class TaskNetworkMemoryUtil {

	/**
	 * Calculate the memory for a task wo communicate with its upstreams and downstreams.
	 * It is BufferPerChannel * (ChannelNumber + SubpartitionNumber) + ExtraBufferForInputGate.
	 * If ExtraBufferForInputGate not set, it will use BufferPerChannel * ChannelNumber
	 *
	 * @param configuration Configuration for the parameters.
	 * @param subPartitionNum The sub partition number of a task.
	 * @param internalResultPartitionNum The internal result partition number of a task.
	 * @param pipelineInputChannelNum The pipeline input channel number of a task.
	 * @param pipelineInputGateNum The pipeline input gate of a task.
	 * @param blockingInputChannelNum The blocking input channel number of a task.
	 * @param blockingInputGateNum The blocking input gate of a task.
	 * @return The network memory needed for the task.
	 */
	public static int calculateTaskNetworkMemory(
		Configuration configuration,
		int subPartitionNum,
		int internalResultPartitionNum,
		int pipelineInputChannelNum,
		int pipelineInputGateNum,
		int blockingInputChannelNum,
		int blockingInputGateNum) {

		final int pageSize = configuration.getInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE);

		final int buffersPerPipelineChannel = configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL);
		final int buffersPerSubpartition = configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION);
		final int extraBuffersPerPipelineGate = configuration.getInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
		final int buffersPerBlockingChannel = configuration.getInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_EXTERNAL_BLOCKING_CHANNEL);
		final int extraBuffersPerBlockingGate = configuration.getInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_EXTERNAL_BLOCKING_GATE);

		int extraBuffersAllPipelineGates;
		if (extraBuffersPerPipelineGate >= 0) {
			extraBuffersAllPipelineGates = extraBuffersPerPipelineGate * pipelineInputGateNum;
		} else {
			extraBuffersAllPipelineGates = buffersPerPipelineChannel * pipelineInputChannelNum;
		}

		int extraBuffersAllBlockingGates = Math.max(extraBuffersPerBlockingGate, 0) * blockingInputGateNum;

		// only internal result partitions need extra buffers, the system does not offer an
		// independent config option for that, num of extra buffers per pipeline gate is used.
		int extraBuffersAllResultPartitions;
		if (extraBuffersPerPipelineGate >= 0) {
			extraBuffersAllResultPartitions = extraBuffersPerPipelineGate * internalResultPartitionNum;
		} else {
			extraBuffersAllResultPartitions = buffersPerSubpartition * subPartitionNum;
		}

		final long totalBuffers = pipelineInputChannelNum * buffersPerPipelineChannel +
			extraBuffersAllPipelineGates + blockingInputChannelNum * buffersPerBlockingChannel +
			extraBuffersAllBlockingGates + subPartitionNum * buffersPerSubpartition + extraBuffersAllResultPartitions;

		long memoryInMbLong = (long) Math.ceil(((double) (pageSize * totalBuffers)) / (1024 * 1024));
		return MathUtils.checkedDownCast(memoryInMbLong);
	}
}
