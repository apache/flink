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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link ResultPartitionDeploymentDescriptor}.
 */
public class ResultPartitionDeploymentDescriptorTest extends TestLogger {
	private static final IntermediateDataSetID resultId = new IntermediateDataSetID();

	private static final IntermediateResultPartitionID partitionId = new IntermediateResultPartitionID();
	private static final ExecutionAttemptID producerExecutionId = new ExecutionAttemptID();

	private static final ResultPartitionType partitionType = ResultPartitionType.PIPELINED;
	private static final int numberOfSubpartitions = 24;
	private static final int connectionIndex = 10;

	private static final PartitionDescriptor partitionDescriptor = new PartitionDescriptor(
		resultId,
		partitionId,
		partitionType,
		numberOfSubpartitions,
		connectionIndex);

	private static final ResultPartitionID resultPartitionID = new ResultPartitionID(partitionId, producerExecutionId);

	private static final ResourceID producerLocation = new ResourceID("producerLocation");
	private static final InetSocketAddress address = new InetSocketAddress("localhost", 10000);
	private static final ConnectionID connectionID = new ConnectionID(address, connectionIndex);

	/**
	 * Tests simple de/serialization with {@link UnknownShuffleDescriptor}.
	 */
	@Test
	public void testSerializationOfUnknownShuffleDescriptor() throws IOException {
		ShuffleDescriptor shuffleDescriptor = new UnknownShuffleDescriptor(resultPartitionID);
		ShuffleDescriptor shuffleDescriptorCopy = CommonTestUtils.createCopySerializable(shuffleDescriptor);
		assertThat(shuffleDescriptorCopy, instanceOf(UnknownShuffleDescriptor.class));
		assertThat(shuffleDescriptorCopy.getResultPartitionID(), is(resultPartitionID));
		assertThat(shuffleDescriptorCopy.isUnknown(), is(true));
	}

	/**
	 * Tests simple de/serialization with {@link NettyShuffleDescriptor}.
	 */
	@Test
	public void testSerializationWithNettyShuffleDescriptor() throws IOException {
		ShuffleDescriptor shuffleDescriptor = new NettyShuffleDescriptor(
			producerLocation,
			new NetworkPartitionConnectionInfo(connectionID),
			resultPartitionID);

		ResultPartitionDeploymentDescriptor copy =
			createCopyAndVerifyResultPartitionDeploymentDescriptor(shuffleDescriptor);

		assertThat(copy.getShuffleDescriptor(), instanceOf(NettyShuffleDescriptor.class));
		NettyShuffleDescriptor shuffleDescriptorCopy = (NettyShuffleDescriptor) copy.getShuffleDescriptor();
		assertThat(shuffleDescriptorCopy.getResultPartitionID(), is(resultPartitionID));
		assertThat(shuffleDescriptorCopy.isUnknown(), is(false));
		assertThat(shuffleDescriptorCopy.isLocalTo(producerLocation), is(true));
		assertThat(shuffleDescriptorCopy.getConnectionId(), is(connectionID));
	}

	private static ResultPartitionDeploymentDescriptor createCopyAndVerifyResultPartitionDeploymentDescriptor(
			ShuffleDescriptor shuffleDescriptor) throws IOException {
		ResultPartitionDeploymentDescriptor orig = new ResultPartitionDeploymentDescriptor(
			partitionDescriptor,
			shuffleDescriptor,
			numberOfSubpartitions,
			true);
		ResultPartitionDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);
		verifyResultPartitionDeploymentDescriptorCopy(copy);
		return copy;
	}

	private static void verifyResultPartitionDeploymentDescriptorCopy(ResultPartitionDeploymentDescriptor copy) {
		assertThat(copy.getResultId(), is(resultId));
		assertThat(copy.getPartitionId(), is(partitionId));
		assertThat(copy.getPartitionType(), is(partitionType));
		assertThat(copy.getNumberOfSubpartitions(), is(numberOfSubpartitions));
		assertThat(copy.sendScheduleOrUpdateConsumersMessage(), is(true));
	}
}
