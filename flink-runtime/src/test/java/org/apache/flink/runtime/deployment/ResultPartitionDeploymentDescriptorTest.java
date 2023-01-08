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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ResultPartitionDeploymentDescriptor}. */
class ResultPartitionDeploymentDescriptorTest {
    private static final IntermediateDataSetID resultId = new IntermediateDataSetID();
    private static final int numberOfPartitions = 5;

    private static final IntermediateResultPartitionID partitionId =
            new IntermediateResultPartitionID();
    private static final ExecutionAttemptID producerExecutionId = createExecutionAttemptId();

    private static final ResultPartitionType partitionType = ResultPartitionType.PIPELINED;
    private static final int numberOfSubpartitions = 24;
    private static final int connectionIndex = 10;
    private static final boolean isBroadcast = false;
    private static final boolean isAllToAllDistribution = true;

    private static final PartitionDescriptor partitionDescriptor =
            new PartitionDescriptor(
                    resultId,
                    numberOfPartitions,
                    partitionId,
                    partitionType,
                    numberOfSubpartitions,
                    connectionIndex,
                    isBroadcast,
                    isAllToAllDistribution);

    private static final ResultPartitionID resultPartitionID =
            new ResultPartitionID(partitionId, producerExecutionId);

    private static final ResourceID producerLocation = new ResourceID("producerLocation");
    private static final InetSocketAddress address = new InetSocketAddress("localhost", 10000);
    private static final ConnectionID connectionID =
            new ConnectionID(producerLocation, address, connectionIndex);

    /** Tests simple de/serialization with {@link UnknownShuffleDescriptor}. */
    @Test
    void testSerializationOfUnknownShuffleDescriptor() throws IOException {
        ShuffleDescriptor shuffleDescriptor = new UnknownShuffleDescriptor(resultPartitionID);
        ShuffleDescriptor shuffleDescriptorCopy =
                CommonTestUtils.createCopySerializable(shuffleDescriptor);
        assertThat(shuffleDescriptorCopy).isInstanceOf(UnknownShuffleDescriptor.class);
        assertThat(resultPartitionID).isEqualTo(shuffleDescriptorCopy.getResultPartitionID());
        assertThat(shuffleDescriptorCopy.isUnknown()).isTrue();
    }

    /** Tests simple de/serialization with {@link NettyShuffleDescriptor}. */
    @Test
    void testSerializationWithNettyShuffleDescriptor() throws IOException {
        ShuffleDescriptor shuffleDescriptor =
                new NettyShuffleDescriptor(
                        producerLocation,
                        new NetworkPartitionConnectionInfo(address, connectionIndex),
                        resultPartitionID);

        ResultPartitionDeploymentDescriptor copy =
                createCopyAndVerifyResultPartitionDeploymentDescriptor(shuffleDescriptor);

        assertThat(copy.getShuffleDescriptor()).isInstanceOf(NettyShuffleDescriptor.class);
        NettyShuffleDescriptor shuffleDescriptorCopy =
                (NettyShuffleDescriptor) copy.getShuffleDescriptor();
        assertThat(resultPartitionID).isEqualTo(shuffleDescriptorCopy.getResultPartitionID());
        assertThat(shuffleDescriptorCopy.isUnknown()).isFalse();
        assertThat(shuffleDescriptorCopy.isLocalTo(producerLocation)).isTrue();
        assertThat(connectionID).isEqualTo(shuffleDescriptorCopy.getConnectionId());
    }

    private static ResultPartitionDeploymentDescriptor
            createCopyAndVerifyResultPartitionDeploymentDescriptor(
                    ShuffleDescriptor shuffleDescriptor) throws IOException {
        ResultPartitionDeploymentDescriptor orig =
                new ResultPartitionDeploymentDescriptor(
                        partitionDescriptor, shuffleDescriptor, numberOfSubpartitions);
        ResultPartitionDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);
        verifyResultPartitionDeploymentDescriptorCopy(copy);
        return copy;
    }

    private static void verifyResultPartitionDeploymentDescriptorCopy(
            ResultPartitionDeploymentDescriptor copy) {
        assertThat(resultId).isEqualTo(copy.getResultId());
        assertThat(numberOfPartitions).isEqualTo(copy.getTotalNumberOfPartitions());
        assertThat(partitionId).isEqualTo(copy.getPartitionId());
        assertThat(partitionType).isEqualTo(copy.getPartitionType());
        assertThat(numberOfSubpartitions).isEqualTo(copy.getNumberOfSubpartitions());
    }
}
