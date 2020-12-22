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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder.STUB_CONNECTION_ID;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link ShuffleDescriptor}.
 */
public class ShuffleDescriptorTest extends TestLogger {

	/**
	 * Tests the deployment descriptors for local, remote, and unknown partition
	 * locations (with lazy deployment allowed and all execution states for the
	 * producers).
	 */
	@Test
	public void testMixedLocalRemoteDeployment() throws Exception {
		ResourceID consumerResourceID = ResourceID.generate();

		// Local and remote channel are only allowed for certain execution
		// states.
		for (ExecutionState state : ExecutionState.values()) {
			if (!isAllowedState(state)) {
				continue;
			}

			ResultPartitionID localPartitionId = new ResultPartitionID();
			ResultPartitionDeploymentDescriptor localPartition =
				createResultPartitionDeploymentDescriptor(localPartitionId, consumerResourceID);

			ResultPartitionID remotePartitionId = new ResultPartitionID();
			ResultPartitionDeploymentDescriptor remotePartition =
				createResultPartitionDeploymentDescriptor(remotePartitionId, ResourceID.generate());

			ShuffleDescriptor localShuffleDescriptor =
				getConsumedPartitionShuffleDescriptor(localPartitionId, state, localPartition);
			ShuffleDescriptor remoteShuffleDescriptor =
				getConsumedPartitionShuffleDescriptor(remotePartitionId, state, remotePartition);

			NettyShuffleDescriptor nettyShuffleDescriptor;

			// Create local or remote channels
			verifyShuffleDescriptor(localShuffleDescriptor, NettyShuffleDescriptor.class, localPartitionId);
			nettyShuffleDescriptor = (NettyShuffleDescriptor) localShuffleDescriptor;
			assertThat(nettyShuffleDescriptor.isLocalTo(consumerResourceID), is(true));

			verifyShuffleDescriptor(remoteShuffleDescriptor, NettyShuffleDescriptor.class, remotePartitionId);
			nettyShuffleDescriptor = (NettyShuffleDescriptor) remoteShuffleDescriptor;
			assertThat(nettyShuffleDescriptor.isLocalTo(consumerResourceID), is(false));
			assertThat(nettyShuffleDescriptor.getConnectionId(), is(STUB_CONNECTION_ID));
		}
	}

	private static boolean isAllowedState(ExecutionState state) {
		return state == ExecutionState.RUNNING ||
			state == ExecutionState.FINISHED ||
			state == ExecutionState.SCHEDULED ||
			state == ExecutionState.DEPLOYING;
	}

	private static void verifyShuffleDescriptor(
			ShuffleDescriptor descriptor,
			Class<? extends ShuffleDescriptor> cl,
			ResultPartitionID partitionID) {
		assertThat(descriptor, instanceOf(cl));
		assertThat(descriptor.getResultPartitionID(), is(partitionID));
	}

	private static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
			ResultPartitionID id,
			ExecutionState state,
			@Nullable ResultPartitionDeploymentDescriptor producedPartition) {
		ShuffleDescriptor shuffleDescriptor = TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(
			id,
			ResultPartitionType.PIPELINED,
			true,
			state,
			producedPartition);
		assertThat(shuffleDescriptor, is(notNullValue()));
		assertThat(shuffleDescriptor.getResultPartitionID(), is(id));
		return shuffleDescriptor;
	}

	private static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
			ResultPartitionID id,
			ResourceID location) throws ExecutionException, InterruptedException {
		ProducerDescriptor producerDescriptor = new ProducerDescriptor(
			location,
			id.getProducerId(),
			STUB_CONNECTION_ID.getAddress().getAddress(),
			STUB_CONNECTION_ID.getAddress().getPort());
		PartitionDescriptor partitionDescriptor = PartitionDescriptorBuilder
			.newBuilder()
			.setPartitionId(id.getPartitionId())
			.build();
		ShuffleDescriptor shuffleDescriptor =
			NettyShuffleMaster.INSTANCE.registerPartitionWithProducer(
				partitionDescriptor,
				producerDescriptor).get();
		return new ResultPartitionDeploymentDescriptor(
			partitionDescriptor,
			shuffleDescriptor,
			1,
			true);
	}
}
