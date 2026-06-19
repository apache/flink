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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleTestUtils;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder.STUB_CONNECTION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ShuffleDescriptor}. */
@ExtendWith(TestLoggerExtension.class)
class ShuffleDescriptorTest {

    /**
     * Tests the deployment descriptors for local, remote, and unknown partition locations (with
     * lazy deployment allowed and all execution states for the producers).
     */
    @Test
    void testMixedLocalRemoteUnknownDeployment() throws Exception {
        ResourceID consumerResourceID = ResourceID.generate();
        JobID jobID = new JobID();

        // Local and remote channel are only allowed for certain execution
        // states.
        for (ExecutionState state : ExecutionState.values()) {
            ResultPartitionID localPartitionId = new ResultPartitionID();
            ResultPartitionDeploymentDescriptor localPartition =
                    createResultPartitionDeploymentDescriptor(
                            jobID, localPartitionId, consumerResourceID);

            ResultPartitionID remotePartitionId = new ResultPartitionID();
            ResourceID remoteResourceID = ResourceID.generate();
            ResultPartitionDeploymentDescriptor remotePartition =
                    createResultPartitionDeploymentDescriptor(
                            jobID, remotePartitionId, remoteResourceID);

            ResultPartitionID unknownPartitionId = new ResultPartitionID();

            ShuffleDescriptor localShuffleDescriptor =
                    getConsumedPartitionShuffleDescriptor(
                            localPartitionId,
                            state,
                            localPartition,
                            TaskDeploymentDescriptorFactory.PartitionLocationConstraint
                                    .CAN_BE_UNKNOWN);
            ShuffleDescriptor remoteShuffleDescriptor =
                    getConsumedPartitionShuffleDescriptor(
                            remotePartitionId,
                            state,
                            remotePartition,
                            TaskDeploymentDescriptorFactory.PartitionLocationConstraint
                                    .CAN_BE_UNKNOWN);
            ShuffleDescriptor unknownShuffleDescriptor =
                    getConsumedPartitionShuffleDescriptor(
                            unknownPartitionId,
                            state,
                            null,
                            TaskDeploymentDescriptorFactory.PartitionLocationConstraint
                                    .CAN_BE_UNKNOWN);

            // These states are allowed
            if (state == ExecutionState.RUNNING
                    || state == ExecutionState.INITIALIZING
                    || state == ExecutionState.FINISHED
                    || state == ExecutionState.SCHEDULED
                    || state == ExecutionState.DEPLOYING) {
                NettyShuffleDescriptor nettyShuffleDescriptor;

                // Create local or remote channels
                verifyShuffleDescriptor(
                        localShuffleDescriptor,
                        NettyShuffleDescriptor.class,
                        false,
                        localPartitionId);
                nettyShuffleDescriptor = (NettyShuffleDescriptor) localShuffleDescriptor;
                assertThat(nettyShuffleDescriptor.isLocalTo(consumerResourceID)).isTrue();

                verifyShuffleDescriptor(
                        remoteShuffleDescriptor,
                        NettyShuffleDescriptor.class,
                        false,
                        remotePartitionId);
                nettyShuffleDescriptor = (NettyShuffleDescriptor) remoteShuffleDescriptor;
                assertThat(nettyShuffleDescriptor.isLocalTo(consumerResourceID)).isFalse();
                assertThat(nettyShuffleDescriptor.getConnectionId().getAddress())
                        .isEqualTo(STUB_CONNECTION_ID.getAddress());
                assertThat(nettyShuffleDescriptor.getConnectionId().getConnectionIndex())
                        .isEqualTo(STUB_CONNECTION_ID.getConnectionIndex());
                assertThat(nettyShuffleDescriptor.getConnectionId().getResourceID())
                        .isEqualTo(remoteResourceID);
            } else {
                // Unknown (lazy deployment allowed)
                verifyShuffleDescriptor(
                        localShuffleDescriptor,
                        UnknownShuffleDescriptor.class,
                        true,
                        localPartitionId);
                verifyShuffleDescriptor(
                        remoteShuffleDescriptor,
                        UnknownShuffleDescriptor.class,
                        true,
                        remotePartitionId);
            }

            verifyShuffleDescriptor(
                    unknownShuffleDescriptor,
                    UnknownShuffleDescriptor.class,
                    true,
                    unknownPartitionId);
        }
    }

    private static void verifyShuffleDescriptor(
            ShuffleDescriptor descriptor,
            Class<? extends ShuffleDescriptor> cl,
            boolean unknown,
            ResultPartitionID partitionID) {
        assertThat(descriptor).isInstanceOf(cl);
        assertThat(descriptor.isUnknown()).isEqualTo(unknown);
        assertThat(descriptor.getResultPartitionID()).isEqualTo(partitionID);
    }

    @Test
    void testNonFinishedHybridPartitionShouldBeUnknown() throws Exception {
        ResultPartitionID unknownPartitionId = new ResultPartitionID();

        ResultPartitionID partitionID = new ResultPartitionID();
        ResourceID remoteResourceID = ResourceID.generate();
        JobID jobID = new JobID();

        ResultPartitionDeploymentDescriptor producerPartition =
                createResultPartitionDeploymentDescriptor(jobID, partitionID, remoteResourceID);

        ShuffleDescriptor unknownSdd =
                getConsumedPartitionShuffleDescriptor(
                        unknownPartitionId,
                        ExecutionState.DEPLOYING,
                        ResultPartitionType.HYBRID_FULL,
                        producerPartition,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.CAN_BE_UNKNOWN,
                        true);
        assertThat(unknownSdd).isInstanceOf(UnknownShuffleDescriptor.class);
        assertThat(unknownSdd.isUnknown()).isTrue();
        assertThat(unknownSdd.getResultPartitionID()).isEqualTo(unknownPartitionId);

        assertThatThrownBy(
                        () ->
                                getConsumedPartitionShuffleDescriptor(
                                        unknownPartitionId,
                                        ExecutionState.DEPLOYING,
                                        ResultPartitionType.HYBRID_FULL,
                                        producerPartition,
                                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint
                                                .MUST_BE_KNOWN,
                                        true))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testUnknownDescriptorWithOrWithoutLazyDeployment() {
        ResultPartitionID unknownPartitionId = new ResultPartitionID();

        // This should work if lazy deployment is allowed
        ShuffleDescriptor unknownSdd =
                getConsumedPartitionShuffleDescriptor(
                        unknownPartitionId,
                        ExecutionState.CREATED,
                        null,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.CAN_BE_UNKNOWN);

        assertThat(unknownSdd).isInstanceOf(UnknownShuffleDescriptor.class);
        assertThat(unknownSdd.isUnknown()).isTrue();
        assertThat(unknownSdd.getResultPartitionID()).isEqualTo(unknownPartitionId);

        assertThatThrownBy(
                        () -> {
                            // Fail if lazy deployment is *not* allowed
                            getConsumedPartitionShuffleDescriptor(
                                    unknownPartitionId,
                                    ExecutionState.CREATED,
                                    null,
                                    TaskDeploymentDescriptorFactory.PartitionLocationConstraint
                                            .MUST_BE_KNOWN);
                        })
                .withFailMessage("Did not throw expected ExecutionGraphException")
                .isInstanceOf(IllegalStateException.class);
    }

    private static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
            ResultPartitionID id,
            ExecutionState state,
            @Nullable ResultPartitionDeploymentDescriptor producedPartition,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint
                    partitionLocationConstraint) {
        return getConsumedPartitionShuffleDescriptor(
                id,
                state,
                ResultPartitionType.PIPELINED,
                producedPartition,
                partitionLocationConstraint,
                false);
    }

    private static ShuffleDescriptor getConsumedPartitionShuffleDescriptor(
            ResultPartitionID id,
            ExecutionState state,
            ResultPartitionType resultPartitionType,
            @Nullable ResultPartitionDeploymentDescriptor producedPartition,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            boolean nonFinishedHybridPartitionShouldBeUnknown) {
        ShuffleDescriptor shuffleDescriptor =
                TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(
                        id,
                        resultPartitionType,
                        true,
                        state,
                        partitionLocationConstraint,
                        producedPartition,
                        nonFinishedHybridPartitionShouldBeUnknown);
        assertThat(shuffleDescriptor).isNotNull();
        assertThat(shuffleDescriptor.getResultPartitionID()).isEqualTo(id);
        return shuffleDescriptor;
    }

    private static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
            JobID jobID, ResultPartitionID id, ResourceID location)
            throws ExecutionException, InterruptedException {
        ProducerDescriptor producerDescriptor =
                new ProducerDescriptor(
                        location,
                        id.getProducerId(),
                        STUB_CONNECTION_ID.getAddress().getAddress(),
                        STUB_CONNECTION_ID.getAddress().getPort());
        PartitionDescriptor partitionDescriptor =
                PartitionDescriptorBuilder.newBuilder().setPartitionId(id.getPartitionId()).build();
        ShuffleDescriptor shuffleDescriptor =
                ShuffleTestUtils.DEFAULT_SHUFFLE_MASTER
                        .registerPartitionWithProducer(
                                jobID, partitionDescriptor, producerDescriptor)
                        .get();
        return new ResultPartitionDeploymentDescriptor(partitionDescriptor, shuffleDescriptor, 1);
    }
}
