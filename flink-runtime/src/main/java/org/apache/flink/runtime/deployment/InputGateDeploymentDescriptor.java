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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.io.Serializable;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a single input gate instance.
 *
 * <p> Each input gate consumes partitions of a single intermediate result. The consumed
 * subpartition index is the same for each consumed partition.
 *
 * @see SingleInputGate
 */
public class InputGateDeploymentDescriptor implements Serializable {

	/**
	 * The ID of the consumed intermediate result. Each input gate consumes partitions of the
	 * intermediate result specified by this ID. This ID also identifies the input gate at the
	 * consuming task.
	 */
	private final IntermediateDataSetID consumedResultId;

	/**
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
	 */
	private final int consumedSubpartitionIndex;

	/** An input channel for each consumed subpartition. */
	private final InputChannelDeploymentDescriptor[] inputChannels;

	public InputGateDeploymentDescriptor(
			IntermediateDataSetID consumedResultId,
			int consumedSubpartitionIndex,
			InputChannelDeploymentDescriptor[] inputChannels) {

		this.consumedResultId = checkNotNull(consumedResultId);

		checkArgument(consumedSubpartitionIndex >= 0);
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;

		this.inputChannels = checkNotNull(inputChannels);
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	public int getConsumedSubpartitionIndex() {
		return consumedSubpartitionIndex;
	}

	public InputChannelDeploymentDescriptor[] getInputChannelDeploymentDescriptors() {
		return inputChannels;
	}

	@Override
	public String toString() {
		return String.format("InputGateDeploymentDescriptor [result id: %s, " +
						"consumed subpartition index: %d, input channels: %s]",
				consumedResultId.toString(), consumedSubpartitionIndex,
				Arrays.toString(inputChannels));
	}
}
