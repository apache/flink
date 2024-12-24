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
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.util.GroupCache;
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Deployment descriptor for a single input gate instance.
 *
 * <p>Each input gate consumes partitions of a single intermediate result. The consumed subpartition
 * index range is the same for each consumed partition.
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
     * Provides information about the number of consumed shuffle descriptors and the mapping between
     * consumed shuffle descriptor ranges and their corresponding subpartition ranges.
     */
    private final ConsumedSubpartitionContext consumedSubpartitionContext;

    /** An input channel for each consumed subpartition. */
    private transient ShuffleDescriptor[] inputChannels;

    /** Serialized value of shuffle descriptors. */
    private final List<MaybeOffloaded<ShuffleDescriptorGroup>> serializedInputChannels;

    /** Number of input channels. */
    private final int numberOfInputChannels;

    @VisibleForTesting
    public InputGateDeploymentDescriptor(
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            @Nonnegative int consumedSubpartitionIndex,
            ShuffleDescriptorAndIndex[] inputChannels)
            throws IOException {
        this(
                consumedResultId,
                consumedPartitionType,
                new IndexRange(consumedSubpartitionIndex, consumedSubpartitionIndex),
                inputChannels.length,
                Collections.singletonList(
                        new NonOffloaded<>(
                                CompressedSerializedValue.fromObject(
                                        new ShuffleDescriptorGroup(inputChannels)))));
    }

    public InputGateDeploymentDescriptor(
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            IndexRange consumedSubpartitionIndexRange,
            int numberOfInputChannels,
            List<MaybeOffloaded<ShuffleDescriptorGroup>> serializedInputChannels) {
        this(
                consumedResultId,
                consumedPartitionType,
                ConsumedSubpartitionContext.buildConsumedSubpartitionContext(
                        numberOfInputChannels, consumedSubpartitionIndexRange),
                numberOfInputChannels,
                serializedInputChannels);
    }

    public InputGateDeploymentDescriptor(
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            ConsumedSubpartitionContext consumedSubpartitionContext,
            int numberOfInputChannels,
            List<MaybeOffloaded<ShuffleDescriptorGroup>> serializedInputChannels) {
        this.consumedResultId = checkNotNull(consumedResultId);
        this.consumedPartitionType = checkNotNull(consumedPartitionType);
        this.consumedSubpartitionContext = checkNotNull(consumedSubpartitionContext);
        this.serializedInputChannels = checkNotNull(serializedInputChannels);
        this.numberOfInputChannels = numberOfInputChannels;
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

    public int getNumConsumedShuffleDescriptors() {
        return consumedSubpartitionContext.getNumConsumedShuffleDescriptors();
    }

    public Collection<IndexRange> getConsumedShuffleDescriptorRanges() {
        return consumedSubpartitionContext.getConsumedShuffleDescriptorRanges();
    }

    public IndexRange getConsumedSubpartitionRange(int shuffleDescriptorIndex) {
        return consumedSubpartitionContext.getConsumedSubpartitionRange(shuffleDescriptorIndex);
    }

    /**
     * Retrieves all {@link ShuffleDescriptor}s associated of this input gate deployment descriptor.
     *
     * <p>Note that the returned descriptors may not be fully consumed. The {@link
     * #getConsumedShuffleDescriptorRanges} method provides the indices of the shuffle descriptors
     * that are really consumed.
     *
     * <p>We need to return all {@link ShuffleDescriptor}s to maintain the mapping between the
     * descriptors and the indices recorded in the {@link ConsumedSubpartitionContext}.
     *
     * @return an array of {@link ShuffleDescriptor}s.
     * @throws RuntimeException if deserialization of shuffle descriptors fails.
     */
    public ShuffleDescriptor[] getShuffleDescriptors() {
        if (inputChannels == null) {
            // This is only for testing scenarios, in a production environment we always call
            // tryLoadAndDeserializeShuffleDescriptors to deserialize ShuffleDescriptors first.
            inputChannels = new ShuffleDescriptor[numberOfInputChannels];
            try {
                for (MaybeOffloaded<ShuffleDescriptorGroup> serializedShuffleDescriptors :
                        serializedInputChannels) {
                    checkState(
                            serializedShuffleDescriptors instanceof NonOffloaded,
                            "Trying to work with offloaded serialized shuffle descriptors.");
                    NonOffloaded<ShuffleDescriptorGroup> nonOffloadedSerializedValue =
                            (NonOffloaded<ShuffleDescriptorGroup>) serializedShuffleDescriptors;
                    tryDeserializeShuffleDescriptorGroup(nonOffloadedSerializedValue);
                }
            } catch (ClassNotFoundException | IOException e) {
                throw new RuntimeException("Could not deserialize shuffle descriptors.", e);
            }
        }
        return inputChannels;
    }

    public void tryLoadAndDeserializeShuffleDescriptors(
            @Nullable PermanentBlobService blobService,
            JobID jobId,
            GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache)
            throws IOException {
        if (inputChannels != null) {
            return;
        }

        try {
            inputChannels = new ShuffleDescriptor[numberOfInputChannels];

            for (MaybeOffloaded<ShuffleDescriptorGroup> serializedShuffleDescriptors :
                    serializedInputChannels) {
                tryLoadAndDeserializeShuffleDescriptorGroup(
                        blobService, jobId, serializedShuffleDescriptors, shuffleDescriptorsCache);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not deserialize shuffle descriptors.", e);
        }
    }

    private void tryLoadAndDeserializeShuffleDescriptorGroup(
            @Nullable PermanentBlobService blobService,
            JobID jobId,
            MaybeOffloaded<ShuffleDescriptorGroup> serializedShuffleDescriptors,
            GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache)
            throws IOException, ClassNotFoundException {
        if (serializedShuffleDescriptors instanceof Offloaded) {
            PermanentBlobKey blobKey =
                    ((Offloaded<ShuffleDescriptorGroup>) serializedShuffleDescriptors)
                            .serializedValueKey;
            ShuffleDescriptorGroup shuffleDescriptorGroup =
                    shuffleDescriptorsCache.get(jobId, blobKey);
            if (shuffleDescriptorGroup == null) {
                Preconditions.checkNotNull(blobService);
                // NOTE: Do not delete the ShuffleDescriptor BLOBs since it may be needed again
                // during
                // recovery. (it is deleted automatically on the BLOB server and cache when its
                // partition is no longer available or the job enters a terminal state)
                CompressedSerializedValue<ShuffleDescriptorGroup> serializedValue =
                        CompressedSerializedValue.fromBytes(blobService.readFile(jobId, blobKey));
                shuffleDescriptorGroup =
                        serializedValue.deserializeValue(getClass().getClassLoader());
                // update cache
                shuffleDescriptorsCache.put(jobId, blobKey, shuffleDescriptorGroup);
            }
            putOrReplaceShuffleDescriptors(shuffleDescriptorGroup);
        } else {
            NonOffloaded<ShuffleDescriptorGroup> nonOffloadedSerializedValue =
                    (NonOffloaded<ShuffleDescriptorGroup>) serializedShuffleDescriptors;
            tryDeserializeShuffleDescriptorGroup(nonOffloadedSerializedValue);
        }
    }

    private void tryDeserializeShuffleDescriptorGroup(
            NonOffloaded<ShuffleDescriptorGroup> nonOffloadedShuffleDescriptorGroup)
            throws IOException, ClassNotFoundException {
        ShuffleDescriptorGroup shuffleDescriptorGroup =
                nonOffloadedShuffleDescriptorGroup.serializedValue.deserializeValue(
                        getClass().getClassLoader());
        putOrReplaceShuffleDescriptors(shuffleDescriptorGroup);
    }

    private void putOrReplaceShuffleDescriptors(ShuffleDescriptorGroup shuffleDescriptorGroup) {
        for (ShuffleDescriptorAndIndex shuffleDescriptorAndIndex :
                shuffleDescriptorGroup.getShuffleDescriptors()) {
            ShuffleDescriptor inputChannelDescriptor =
                    inputChannels[shuffleDescriptorAndIndex.getIndex()];
            if (inputChannelDescriptor != null) {
                checkState(
                        inputChannelDescriptor.isUnknown(),
                        "Only unknown shuffle descriptor can be replaced.");
            }
            inputChannels[shuffleDescriptorAndIndex.getIndex()] =
                    shuffleDescriptorAndIndex.getShuffleDescriptor();
        }
    }

    @Override
    public String toString() {
        return String.format(
                "InputGateDeploymentDescriptor [result id: %s, "
                        + "consumed subpartition context: %s]",
                consumedResultId.toString(), consumedSubpartitionContext.toString());
    }
}
