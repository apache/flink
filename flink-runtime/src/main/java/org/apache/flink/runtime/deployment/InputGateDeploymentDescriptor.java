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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.NonOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.Offloaded;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a single input gate instance.
 *
 * <p>Each input gate consumes partitions of a single intermediate result. The consumed subpartition
 * index is the same for each consumed partition.
 *
 * @see SingleInputGate
 */
public class InputGateDeploymentDescriptor implements Serializable {

    private static final long serialVersionUID = -7143441863165366704L;
    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    private final IntermediateDataSetID consumedResultId;

    /** The type of the partition the input gate is going to consume. */
    private final ResultPartitionType consumedPartitionType;

    /**
     * The index of the consumed subpartition of each consumed partition. This index depends on the
     * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
     */
    @Nonnegative private final int consumedSubpartitionIndex;

    /** An input channel for each consumed subpartition. */
    private transient ShuffleDescriptor[] inputChannels;

    /** Serialized value of shuffle descriptors. */
    private MaybeOffloaded<ShuffleDescriptor[]> serializedInputChannels;

    @VisibleForTesting
    public InputGateDeploymentDescriptor(
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            @Nonnegative int consumedSubpartitionIndex,
            ShuffleDescriptor[] inputChannels)
            throws IOException {
        this(
                consumedResultId,
                consumedPartitionType,
                consumedSubpartitionIndex,
                new NonOffloaded<>(CompressedSerializedValue.fromObject(inputChannels)));
    }

    public InputGateDeploymentDescriptor(
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            @Nonnegative int consumedSubpartitionIndex,
            MaybeOffloaded<ShuffleDescriptor[]> serializedInputChannels) {
        this.consumedResultId = checkNotNull(consumedResultId);
        this.consumedPartitionType = checkNotNull(consumedPartitionType);
        this.consumedSubpartitionIndex = consumedSubpartitionIndex;
        this.serializedInputChannels = checkNotNull(serializedInputChannels);
    }

    public IntermediateDataSetID getConsumedResultId() {
        return consumedResultId;
    }

    /**
     * Returns the type of this input channel's consumed result partition.
     *
     * @return consumed result partition type
     */
    public ResultPartitionType getConsumedPartitionType() {
        return consumedPartitionType;
    }

    @Nonnegative
    public int getConsumedSubpartitionIndex() {
        return consumedSubpartitionIndex;
    }

    public void loadBigData(@Nullable PermanentBlobService blobService, JobID jobId)
            throws IOException, ClassNotFoundException {
        if (serializedInputChannels instanceof Offloaded) {
            PermanentBlobKey blobKey =
                    ((Offloaded<ShuffleDescriptor[]>) serializedInputChannels).serializedValueKey;

            Preconditions.checkNotNull(blobService);

            // NOTE: Do not delete the ShuffleDescriptor BLOBs since it may be needed again during
            // recovery. (it is deleted automatically on the BLOB server and cache when its
            // partition is no longer available or the job enters a terminal state)
            CompressedSerializedValue<ShuffleDescriptor[]> serializedValue =
                    CompressedSerializedValue.fromBytes(blobService.readFile(jobId, blobKey));
            serializedInputChannels = new NonOffloaded<>(serializedValue);

            Preconditions.checkNotNull(serializedInputChannels);
        }
    }

    public ShuffleDescriptor[] getShuffleDescriptors() {
        try {
            if (inputChannels == null) {
                if (serializedInputChannels instanceof NonOffloaded) {
                    NonOffloaded<ShuffleDescriptor[]> nonOffloadedSerializedValue =
                            (NonOffloaded<ShuffleDescriptor[]>) serializedInputChannels;
                    inputChannels =
                            nonOffloadedSerializedValue.serializedValue.deserializeValue(
                                    getClass().getClassLoader());
                } else {
                    throw new IllegalStateException(
                            "Trying to work with offloaded serialized shuffle descriptors.");
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Could not deserialize shuffle descriptors.", e);
        }
        return inputChannels;
    }

    @Override
    public String toString() {
        return String.format(
                "InputGateDeploymentDescriptor [result id: %s, "
                        + "consumed subpartition index: %d, input channels: %s]",
                consumedResultId.toString(),
                consumedSubpartitionIndex,
                Arrays.toString(getShuffleDescriptors()));
    }
}
