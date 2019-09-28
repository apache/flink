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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder.STUB_CONNECTION_ID;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
	public void testMixedLocalRemoteUnknownDeployment() throws Exception {
		ResourceID consumerResourceID = ResourceID.generate();

		// Local and remote channel are only allowed for certain execution
		// states.
		for (ExecutionState state : ExecutionState.values()) {
			ResultPartitionID localPartitionId = new ResultPartitionID();
			ResultPartitionDeploymentDescriptor localPartition =
				createResultPartitionDeploymentDescriptor(localPartitionId, consumerResourceID);

			ResultPartitionID remotePartitionId = new ResultPartitionID();
			ResultPartitionDeploymentDescriptor remotePartition =
				createResultPartitionDeploymentDescriptor(remotePartitionId, ResourceID.generate());

			ResultPartitionID unknownPartitionId = new ResultPartitionID();

			ShuffleDescriptor localShuffleDescriptor =
				getConsumedPartitionShuffleDescriptor(localPartitionId, state, localPartition, true);
			ShuffleDescriptor remoteShuffleDescriptor =
				getConsumedPartitionShuffleDescriptor(remotePartitionId, state, remotePartition, true);
			ShuffleDescriptor unknownShuffleDescriptor =
				getConsumedPartitionShuffleDescriptor(unknownPartitionId, state, null, true);

			// These states are allowed
			if (state == ExecutionState.RUNNING ||
				state == ExecutionState.FINISHED ||
				state == ExecutionState.SCHEDULED ||
				state == ExecutionState.DEPLOYING) {
				NettyShuffleDescriptor nettyShuffleDescriptor;

				// Create local or remote channels
				verifyShuffleDescriptor(localShuffleDescriptor, NettyShuffleDescriptor.class, false, localPartitionId);
				nettyShuffleDescriptor = (NettyShuffleDescriptor) localShuffleDescriptor;
				assertThat(nettyShuffleDescriptor.isLocalTo(consumerResourceID), is(true));

				verifyShuffleDescriptor(remoteShuffleDescriptor, NettyShuffleDescriptor.class, false, remotePartitionId);
				nettyShuffleDescriptor = (NettyShuffleDescriptor) remoteShuffleDescriptor;
				assertThat(nettyShuffleDescriptor.isLocalTo(consumerResourceID), is(false));
				assertThat(nettyShuffleDescriptor.getConnectionId(), is(STUB_CONNECTION_ID));
			} else {
				// Unknown (lazy deployment allowed)
				verifyShuffleDescriptor(localShuffleDescriptor, UnknownShuffleDescriptor.class, true, localPartitionId);
				verifyShuffleDescriptor(remoteShuffleDescriptor, UnknownShuffleDescriptor.class, true, remotePartitionId);
			}

			verifyShuffleDescriptor(unknownShuffleDescriptor, UnknownShuffleDescriptor.class, true, unknownPartitionId);
		}
	}

	private static void verifyShuffleDescriptor(
			ShuffleDescriptor descriptor,
			Class<? extends ShuffleDescriptor> cl,
			boolean unknown,
			ResultPartitionID partitionID) {
		assertThat(descriptor, instanceOf(cl));
		assertThat(descriptor.isUnknown(), is(unknown));
		assertThat(descriptor.getResultPartitionID(), is(partitionID));
	}

	@Test
	public void testUnknownDescriptorWithOrWithoutLazyDeployment() {
		ResultPartitionID unknownPartitionId = new ResultPartitionID();

		// This should work if lazy deployment is allowed
		ShuffleDescriptor unknownSdd = getConsumedPartitionShuffleDescriptor(
			unknownPartitionId,
			ExecutionState.CREATED,
			null,
			true);

		assertThat(unknownSdd, instanceOf(UnknownShuffleDescriptor.class));
		assertThat(unknownSdd.isUnknown(), is(true));
		assertThat(unknownSdd.getResultPartitionID(), is(unknownPartitionId));

		try {
			// Fail if lazy deployment is *not* allowed
			getConsumedPartitionShuffleDescriptor(
				unknownPartitionId,
				ExecutionState.CREATED,
				null,
				false);
			fail("Did not throw expected ExecutionGraphException");
		} catch (IllegalStateException ignored) {
		}
	}

	private static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
			ResultPartitionID id,
			ExecutionState state,
			@Nullable ResultPartitionDeploymentDescriptor producedPartition,
			boolean allowLazyDeployment) {
		ShuffleDescriptor shuffleDescriptor = TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(
			id,
			ResultPartitionType.PIPELINED,
			true,
			state,
			allowLazyDeployment,
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
		PartitionDescriptor partitionDescriptor = new PartitionDescriptor(
			new IntermediateDataSetID(),
			id.getPartitionId(),
			ResultPartitionType.PIPELINED,
			1,
			0);
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
