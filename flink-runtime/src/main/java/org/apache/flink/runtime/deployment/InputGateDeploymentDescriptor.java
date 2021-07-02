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
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nonnegative;

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
    private final SerializedValue<ShuffleDescriptor[]> serializedInputChannels;

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
                CompressedSerializedValue.fromObject(inputChannels));
    }

    public InputGateDeploymentDescriptor(
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            @Nonnegative int consumedSubpartitionIndex,
            SerializedValue<ShuffleDescriptor[]> serializedInputChannels) {
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

    public ShuffleDescriptor[] getShuffleDescriptors() {
        try {
            if (inputChannels == null) {
                if (serializedInputChannels != null) {
                    inputChannels =
                            serializedInputChannels.deserializeValue(getClass().getClassLoader());
                } else {
                    throw new IllegalStateException(
                            "No input channel available in this InputChannelDeploymentDescriptor.");
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
