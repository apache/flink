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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder.createRemoteWithIdAndLocation;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link NettyShuffleMaster}.
 */
public class NettyShuffleUtilsTest extends TestLogger {

	/**
	 * This test verifies that the {@link NettyShuffleEnvironment} requires
	 * buffers as expected, so that the required shuffle memory size returned by
	 * {@link ShuffleMaster#getShuffleMemoryForTask(TaskInputsOutputsDescriptor)}
	 * is correct.
	 */
	@Test
	public void testComputeRequiredNetworkBuffers() throws Exception {
		final int numTotalBuffers = 1000;
		final int numBuffersPerChannel = 5;
		final NettyShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder()
			.setNumNetworkBuffers(numTotalBuffers)
			.setNetworkBuffersPerChannel(numBuffersPerChannel)
			.build();

		final int numChannels1 = 3;
		final SingleInputGate inputGate1 = createInputGate(
			shuffleEnvironment,
			ResultPartitionType.PIPELINED_BOUNDED,
			numChannels1);
		inputGate1.setupBuffers();

		final int numChannels2 = 4;
		final SingleInputGate inputGate2 = createInputGate(
			shuffleEnvironment,
			ResultPartitionType.BLOCKING,
			numChannels2);
		inputGate2.setupBuffers();

		final int numSubpartitions1 = 5;
		final ResultPartition resultPartition1 = createResultPartition(
			shuffleEnvironment,
			ResultPartitionType.PIPELINED_BOUNDED,
			numSubpartitions1);
		resultPartition1.setup();

		final int numSubpartitions2 = 6;
		final ResultPartition resultPartition2 = createResultPartition(
			shuffleEnvironment,
			ResultPartitionType.BLOCKING,
			numSubpartitions2);
		resultPartition2.setup();

		assertEquals(
			NettyShuffleUtils.computeRequiredNetworkBuffers(
				numBuffersPerChannel,
				Arrays.asList(numChannels1, numChannels2),
				Arrays.asList(numSubpartitions1, numSubpartitions2)),
			shuffleEnvironment.getNetworkBufferPool().getNumTotalRequiredBuffers());
	}

	private static SingleInputGate createInputGate(
		final NettyShuffleEnvironment network,
		final ResultPartitionType resultPartitionType,
		final int numInputChannels) {

		final ShuffleDescriptor[] shuffleDescriptors = new NettyShuffleDescriptor[numInputChannels];
		for (int i = 0; i < numInputChannels; i++) {
			shuffleDescriptors[i] = createRemoteWithIdAndLocation(
				new IntermediateResultPartitionID(),
				ResourceID.generate());
		}

		final InputGateDeploymentDescriptor inputGateDeploymentDescriptor = new InputGateDeploymentDescriptor(
			new IntermediateDataSetID(),
			resultPartitionType,
			0,
			shuffleDescriptors);

		final ExecutionAttemptID consumerID = new ExecutionAttemptID();
		final Collection<SingleInputGate> inputGates = network.createInputGates(
			network.createShuffleIOOwnerContext("", consumerID, new UnregisteredMetricsGroup()),
			SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
			Collections.singleton(inputGateDeploymentDescriptor));

		return inputGates.iterator().next();
	}

	private static ResultPartition createResultPartition(
		final NettyShuffleEnvironment network,
		final ResultPartitionType resultPartitionType,
		final int numSubpartitions) {

		final ShuffleDescriptor shuffleDescriptor = createRemoteWithIdAndLocation(
			new IntermediateResultPartitionID(),
			ResourceID.generate());

		final PartitionDescriptor partitionDescriptor = PartitionDescriptorBuilder
			.newBuilder()
			.setPartitionId(shuffleDescriptor.getResultPartitionID().getPartitionId())
			.setNumberOfSubpartitions(numSubpartitions)
			.setPartitionType(resultPartitionType)
			.build();
		final ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor = new ResultPartitionDeploymentDescriptor(
			partitionDescriptor,
			shuffleDescriptor,
			1,
			true);

		final ExecutionAttemptID consumerID = new ExecutionAttemptID();
		final Collection<ResultPartition> resultPartitions = network.createResultPartitionWriters(
			network.createShuffleIOOwnerContext("", consumerID, new UnregisteredMetricsGroup()),
			Collections.singleton(resultPartitionDeploymentDescriptor));

		return resultPartitions.iterator().next();
	}
}
