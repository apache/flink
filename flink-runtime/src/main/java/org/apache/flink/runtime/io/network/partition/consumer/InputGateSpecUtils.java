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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Utils to manage the specs of the {@link InputGate}, for example, {@link GateBuffersSpec}. */
public class InputGateSpecUtils {

    public static final int DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_BATCH = 1000;

    public static final int DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_STREAM = Integer.MAX_VALUE;

    public static GateBuffersSpec createGateBuffersSpec(
            Optional<Integer> configuredMaxRequiredBuffersPerGate,
            int configuredNetworkBuffersPerChannel,
            int configuredFloatingNetworkBuffersPerGate,
            ResultPartitionType partitionType,
            int numInputChannels,
            boolean enableTieredStorage) {
        int maxRequiredBuffersThresholdPerGate =
                getEffectiveMaxRequiredBuffersPerGate(
                        partitionType, configuredMaxRequiredBuffersPerGate, enableTieredStorage);
        int targetRequiredBuffersPerGate =
                getRequiredBuffersTargetPerGate(
                        numInputChannels, configuredNetworkBuffersPerChannel);
        int targetTotalBuffersPerGate =
                getTotalBuffersTargetPerGate(
                        numInputChannels,
                        configuredNetworkBuffersPerChannel,
                        configuredFloatingNetworkBuffersPerGate);
        int requiredBuffersPerGate =
                Math.min(maxRequiredBuffersThresholdPerGate, targetRequiredBuffersPerGate);

        int effectiveExclusiveBuffersPerChannel =
                getExclusiveBuffersPerChannel(
                        configuredNetworkBuffersPerChannel,
                        numInputChannels,
                        requiredBuffersPerGate);
        int effectiveExclusiveBuffersPerGate =
                getEffectiveExclusiveBuffersPerGate(
                        numInputChannels, effectiveExclusiveBuffersPerChannel);

        int requiredFloatingBuffers = requiredBuffersPerGate - effectiveExclusiveBuffersPerGate;
        int totalFloatingBuffers = targetTotalBuffersPerGate - effectiveExclusiveBuffersPerGate;

        checkState(requiredFloatingBuffers > 0, "Must be positive.");
        checkState(
                requiredFloatingBuffers <= totalFloatingBuffers,
                "Wrong number of floating buffers.");

        return new GateBuffersSpec(
                effectiveExclusiveBuffersPerChannel,
                requiredFloatingBuffers,
                totalFloatingBuffers,
                targetTotalBuffersPerGate);
    }

    @VisibleForTesting
    static int getEffectiveMaxRequiredBuffersPerGate(
            ResultPartitionType partitionType,
            Optional<Integer> configuredMaxRequiredBuffersPerGate,
            boolean enableTieredStorage) {
        return configuredMaxRequiredBuffersPerGate.orElseGet(
                () ->
                        partitionType.isPipelinedOrPipelinedBoundedResultPartition()
                                        // hybrid partition may calculate a backlog that is larger
                                        // than the accurate value. If all buffers are floating, it
                                        // will seriously affect the performance.
                                        || (partitionType.isHybridResultPartition()
                                                && !enableTieredStorage)
                                ? DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_STREAM
                                : DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_BATCH);
    }

    /**
     * Since at least one floating buffer is required, the number of required buffers is reduced by
     * 1, and then the average number of buffers per channel is calculated. Returning the minimum
     * value to ensure that the number of required buffers per gate is not more than the given
     * requiredBuffersPerGate.}.
     */
    private static int getExclusiveBuffersPerChannel(
            int configuredNetworkBuffersPerChannel,
            int numInputChannels,
            int requiredBuffersPerGate) {
        checkArgument(numInputChannels > 0, "Must be positive.");
        checkArgument(requiredBuffersPerGate >= 1, "Require at least 1 buffer per gate.");
        return Math.min(
                configuredNetworkBuffersPerChannel,
                (requiredBuffersPerGate - 1) / numInputChannels);
    }

    private static int getRequiredBuffersTargetPerGate(
            int numInputChannels, int configuredNetworkBuffersPerChannel) {
        return numInputChannels * configuredNetworkBuffersPerChannel + 1;
    }

    private static int getTotalBuffersTargetPerGate(
            int numInputChannels,
            int configuredNetworkBuffersPerChannel,
            int configuredFloatingBuffersPerGate) {
        return numInputChannels * configuredNetworkBuffersPerChannel
                + configuredFloatingBuffersPerGate;
    }

    private static int getEffectiveExclusiveBuffersPerGate(
            int numInputChannels, int effectiveExclusiveBuffersPerChannel) {
        return effectiveExclusiveBuffersPerChannel * numInputChannels;
    }
}
