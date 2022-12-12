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

import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utils to calculate network memory requirement of a vertex from network configuration and details
 * of input and output. The methods help to decide the volume of buffer pools when initializing
 * shuffle environment and also guide network memory announcing in fine-grained resource management.
 */
public class NettyShuffleUtils {

    public static final int DEFAULT_MAX_BUFFERS_PER_GATE_FOR_BLOCKING = 1000;

    public static final int DEFAULT_MAX_BUFFERS_PER_GATE_FOR_STREAM = Integer.MAX_VALUE;

    /**
     * Calculates and returns the number of required exclusive network buffers per input channel.
     */
    public static int getNetworkBuffersPerInputChannel(
            final int configuredNetworkBuffersPerChannel) {
        return configuredNetworkBuffersPerChannel;
    }

    /**
     * Calculates and returns the floating network buffer pool size used by the input gate. The
     * left/right value of the returned pair represent the min/max buffers require by the pool.
     */
    public static Pair<Integer, Integer> getMinMaxFloatingBuffersPerInputGate(
            final int numFloatingBuffersPerGate) {
        // We should guarantee at-least one floating buffer for local channel state recovery.
        return Pair.of(1, numFloatingBuffersPerGate);
    }

    /**
     * Calculates and returns local network buffer pool size used by the result partition. The
     * left/right value of the returned pair represent the min/max buffers require by the pool.
     */
    public static Pair<Integer, Integer> getMinMaxNetworkBuffersPerResultPartition(
            final int configuredNetworkBuffersPerChannel,
            final int numFloatingBuffersPerGate,
            final int sortShuffleMinParallelism,
            final int sortShuffleMinBuffers,
            final int numSubpartitions,
            final ResultPartitionType type) {
        boolean isSortShuffle =
                type.isBlockingOrBlockingPersistentResultPartition()
                        && numSubpartitions >= sortShuffleMinParallelism;
        int min = isSortShuffle ? sortShuffleMinBuffers : numSubpartitions + 1;
        int max =
                type.isBounded()
                        ? numSubpartitions * configuredNetworkBuffersPerChannel
                                + numFloatingBuffersPerGate
                        : (isSortShuffle
                                ? Math.max(min, 4 * numSubpartitions)
                                : NetworkBufferPool.UNBOUNDED_POOL_SIZE);
        // for each upstream hash-based blocking/pipelined subpartition, at least one buffer is
        // needed even the configured network buffers per channel is 0 and this behavior is for
        // performance. If it's not guaranteed that each subpartition can get at least one buffer,
        // more partial buffers with little data will be outputted to network/disk and recycled to
        // be used by other subpartitions which can not get a buffer for data caching.
        return Pair.of(min, Math.max(min, max));
    }

    public static int computeNetworkBuffersForAnnouncing(
            final int numBuffersPerChannel,
            final int numFloatingBuffersPerGate,
            final boolean isGateRequiredMaxBuffersConfigured,
            final int requiredMaxBuffersPerGate,
            final int sortShuffleMinParallelism,
            final int sortShuffleMinBuffers,
            final int numTotalInputGates,
            final Map<IntermediateDataSetID, Integer> inputChannelNums,
            final Map<IntermediateDataSetID, Integer> subpartitionNums,
            final Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes,
            final Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {

        int requirementForInputs = 0;
        int maxSingleGateBuffers = 0;
        for (IntermediateDataSetID dataSetId : inputChannelNums.keySet()) {
            int numChannels = inputChannelNums.get(dataSetId);
            ResultPartitionType inputPartitionType = inputPartitionTypes.get(dataSetId);
            checkNotNull(inputPartitionType);

            int numSingleGateBuffers =
                    getNumBuffersToAnnounceForInputGates(
                            inputPartitionType,
                            numBuffersPerChannel,
                            numFloatingBuffersPerGate,
                            isGateRequiredMaxBuffersConfigured,
                            requiredMaxBuffersPerGate,
                            numChannels);
            requirementForInputs += numSingleGateBuffers;
            maxSingleGateBuffers = Math.max(maxSingleGateBuffers, numSingleGateBuffers);
        }
        requirementForInputs +=
                getReusePartitionInputBuffers(
                        numTotalInputGates, inputChannelNums, maxSingleGateBuffers);

        int requirementForOutputs = 0;
        for (IntermediateDataSetID dataSetId : subpartitionNums.keySet()) {
            int numSubs = subpartitionNums.get(dataSetId);
            ResultPartitionType partitionType = partitionTypes.get(dataSetId);
            checkNotNull(partitionType);

            requirementForOutputs +=
                    getNumBuffersToAnnounceForResultPartition(
                            partitionType,
                            numBuffersPerChannel,
                            numFloatingBuffersPerGate,
                            sortShuffleMinParallelism,
                            sortShuffleMinBuffers,
                            numSubs);
        }

        return requirementForInputs + requirementForOutputs;
    }

    public static int maxRequiredBuffersPerGate(
            ResultPartitionType partitionType,
            boolean isGateRequiredMaxBuffersConfigured,
            int requiredMaxBuffersPerGate) {
        int requiredMaxBuffers;
        if (isGateRequiredMaxBuffersConfigured) {
            requiredMaxBuffers = requiredMaxBuffersPerGate;
        } else {
            requiredMaxBuffers =
                    partitionType.isBlockingOrBlockingPersistentResultPartition()
                            ? DEFAULT_MAX_BUFFERS_PER_GATE_FOR_BLOCKING
                            : DEFAULT_MAX_BUFFERS_PER_GATE_FOR_STREAM;
        }
        checkState(requiredMaxBuffers >= 0, "Max required buffers per gate must be non-negative.");
        return requiredMaxBuffers;
    }

    public static int getMaxFloatingBuffersInGate(
            int numInputChannels, int networkBuffersPerChannel, int floatingNetworkBuffersPerGate) {
        return getExclusiveBuffersInGate(numInputChannels, networkBuffersPerChannel)
                + floatingNetworkBuffersPerGate;
    }

    private static int getNumBuffersToAnnounceForInputGates(
            ResultPartitionType type,
            int numBuffersPerChannel,
            int numFloatingBuffersPerGate,
            boolean isGateRequiredMaxBuffersConfigured,
            int requiredMaxBuffersPerGate,
            int numInputChannels) {
        int maxGateBuffersThreshold =
                maxRequiredBuffersPerGate(
                        type, isGateRequiredMaxBuffersConfigured, requiredMaxBuffersPerGate);

        int adjustedBuffersPerChannel =
                adjustExclusiveBuffersPerChannel(
                        numBuffersPerChannel, numInputChannels, maxGateBuffersThreshold);
        boolean useFloatingBuffer = adjustedBuffersPerChannel < 0;

        int maxFloatingBuffers =
                useFloatingBuffer
                        ? getMaxFloatingBuffersInGate(
                                numInputChannels, numBuffersPerChannel, numFloatingBuffersPerGate)
                        : numFloatingBuffersPerGate;
        int exclusiveBuffersPerChannel = useFloatingBuffer ? 0 : adjustedBuffersPerChannel;

        return exclusiveBuffersPerChannel * numInputChannels + maxFloatingBuffers;
    }

    /**
     * Adjusting the exclusive network buffers based on whether the total exclusive buffers in one
     * gate has exceeded the gate buffer threshold.
     *
     * @return Adjusted buffers or -1. Return -1 if and only if the total exclusive buffers will
     *     exceed the gate buffer threshold though the exclusive network buffers per channel is 1.
     *     Returning -1 indicates the read total buffers in gate should use floating buffers.
     */
    public static int adjustExclusiveBuffersPerChannel(
            int numBuffersPerChannel, int numInputChannels, int maxGateBuffersThreshold) {
        int adjustedBuffersPerChannel = -1;
        for (int i = numBuffersPerChannel; i > 0; i--) {
            if (getExclusiveBuffersInGate(numInputChannels, i) <= maxGateBuffersThreshold) {
                adjustedBuffersPerChannel = i;
                break;
            }
        }
        return adjustedBuffersPerChannel;
    }

    private static int getExclusiveBuffersInGate(
            int numInputChannels, int networkBuffersPerChannel) {
        return networkBuffersPerChannel * numInputChannels;
    }

    private static int getReusePartitionInputBuffers(
            int numTotalInputGates,
            Map<IntermediateDataSetID, Integer> inputChannelNums,
            int maxSingleGateBuffers) {
        checkState(numTotalInputGates >= inputChannelNums.size());
        if (numTotalInputGates == inputChannelNums.size()) {
            return 0;
        }
        return (numTotalInputGates - inputChannelNums.size()) * maxSingleGateBuffers;
    }

    private static int getNumBuffersToAnnounceForResultPartition(
            ResultPartitionType type,
            int configuredNetworkBuffersPerChannel,
            int floatingBuffersPerGate,
            int sortShuffleMinParallelism,
            int sortShuffleMinBuffers,
            int numSubpartitions) {

        Pair<Integer, Integer> minAndMax =
                getMinMaxNetworkBuffersPerResultPartition(
                        configuredNetworkBuffersPerChannel,
                        floatingBuffersPerGate,
                        sortShuffleMinParallelism,
                        sortShuffleMinBuffers,
                        numSubpartitions,
                        type);

        // In order to avoid network buffer request timeout (see FLINK-12852), we announce
        // network buffer requirement by below:
        // 1. For canBePipelined shuffle, the floating buffers may not be returned in time due to
        // back pressure so we need to include all the floating buffers in the announcement, i.e. we
        // should take the max value;
        // 2. For blocking shuffle, it is back pressure free and floating buffers can be recycled
        // in time, so that the minimum required buffers would be enough.
        int ret = type.canBePipelinedConsumed() ? minAndMax.getRight() : minAndMax.getLeft();

        if (ret == Integer.MAX_VALUE) {
            // Should never reach this branch. Result partition will allocate an unbounded
            // buffer pool only when type is ResultPartitionType.PIPELINED. But fine-grained
            // resource management is disabled in such case.
            throw new IllegalArgumentException(
                    "Illegal to announce network memory requirement as Integer.MAX_VALUE, partition type: "
                            + type);
        }
        return ret;
    }

    /** Private default constructor to avoid being instantiated. */
    private NettyShuffleUtils() {}
}
