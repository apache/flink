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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.consumer.InputGateSpecUtils.DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_BATCH;
import static org.apache.flink.runtime.io.network.partition.consumer.InputGateSpecUtils.DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_STREAM;
import static org.apache.flink.runtime.io.network.partition.consumer.InputGateSpecUtils.getEffectiveMaxRequiredBuffersPerGate;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GateBuffersSpec}. */
class GateBuffersSpecTest {

    private static ResultPartitionType[] parameters() {
        return ResultPartitionType.values();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testCalculationWithSufficientRequiredBuffers(ResultPartitionType partitionType) {
        int numInputChannels = 499;
        GateBuffersSpec gateBuffersSpec = createGateBuffersSpec(numInputChannels, partitionType);

        int numExclusivePerChannel = 2;
        int expectedBuffersPerGate = 999;
        int maxBuffersPerGate = 1006;

        checkBuffersInGate(
                gateBuffersSpec,
                numExclusivePerChannel,
                expectedBuffersPerGate,
                expectedBuffersPerGate,
                maxBuffersPerGate);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testCalculationWithOneExclusiveBuffer(ResultPartitionType partitionType) {
        int numInputChannels = 500;
        GateBuffersSpec gateBuffersSpec = createGateBuffersSpec(numInputChannels, partitionType);

        boolean isPipeline = isPipelinedOrHybridResultPartition(partitionType);
        int numExclusivePerChannel = isPipelinedOrHybridResultPartition(partitionType) ? 2 : 1;
        int expectedBuffersPerGate = isPipeline ? 1001 : 1000;
        int maxBuffersPerGate = 1008;

        checkBuffersInGate(
                gateBuffersSpec,
                numExclusivePerChannel,
                expectedBuffersPerGate,
                expectedBuffersPerGate,
                maxBuffersPerGate);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testUpperBoundaryCalculationWithOneExclusiveBuffer(ResultPartitionType partitionType) {
        int numInputChannels = 999;
        GateBuffersSpec gateBuffersSpec = createGateBuffersSpec(numInputChannels, partitionType);

        int numExclusivePerChannel = isPipelinedOrHybridResultPartition(partitionType) ? 2 : 1;
        int expectedBuffersPerGate =
                isPipelinedOrHybridResultPartition(partitionType) ? 1999 : 1000;
        int maxBuffersPerGate = 2006;

        checkBuffersInGate(
                gateBuffersSpec,
                numExclusivePerChannel,
                expectedBuffersPerGate,
                expectedBuffersPerGate,
                maxBuffersPerGate);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testBoundaryCalculationWithoutExclusiveBuffer(ResultPartitionType partitionType) {
        int numInputChannels = 1000;
        GateBuffersSpec gateBuffersSpec = createGateBuffersSpec(numInputChannels, partitionType);

        boolean isPipeline = isPipelinedOrHybridResultPartition(partitionType);
        int numExclusivePerChannel = isPipeline ? 2 : 0;
        int expectedBuffersPerGate = isPipeline ? 2001 : 1000;
        int maxBuffersPerGate = 2008;

        checkBuffersInGate(
                gateBuffersSpec,
                numExclusivePerChannel,
                expectedBuffersPerGate,
                expectedBuffersPerGate,
                maxBuffersPerGate);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testCalculationWithConfiguredZeroExclusiveBuffer(ResultPartitionType partitionType) {
        int numInputChannels = 1001;
        int numExclusiveBuffersPerChannel = 0;
        GateBuffersSpec gateBuffersSpec =
                createGateBuffersSpec(
                        numInputChannels, partitionType, numExclusiveBuffersPerChannel);

        int numExclusivePerChannel = 0;
        int minBuffersPerGate = 1;
        int maxBuffersPerGate = 8;

        checkBuffersInGate(
                gateBuffersSpec,
                numExclusivePerChannel,
                minBuffersPerGate,
                minBuffersPerGate,
                maxBuffersPerGate);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testConfiguredMaxRequiredBuffersPerGate(ResultPartitionType partitionType) {
        boolean enabledTieredStorage = false;
        Optional<Integer> configuredMaxRequiredBuffers = Optional.of(100);
        int effectiveMaxRequiredBuffers =
                getEffectiveMaxRequiredBuffersPerGate(
                        partitionType, configuredMaxRequiredBuffers, enabledTieredStorage);
        assertThat(effectiveMaxRequiredBuffers).isEqualTo(configuredMaxRequiredBuffers.get());
        enabledTieredStorage = true;
        effectiveMaxRequiredBuffers =
                getEffectiveMaxRequiredBuffersPerGate(
                        partitionType, configuredMaxRequiredBuffers, enabledTieredStorage);
        assertThat(effectiveMaxRequiredBuffers).isEqualTo(configuredMaxRequiredBuffers.get());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testDefaultMaxRequiredBuffersPerGate(ResultPartitionType partitionType) {
        Optional<Integer> emptyConfig = Optional.empty();
        boolean enabledTieredStorage = false;
        int effectiveMaxRequiredBuffers =
                getEffectiveMaxRequiredBuffersPerGate(
                        partitionType, emptyConfig, enabledTieredStorage);
        int expectEffectiveMaxRequiredBuffers =
                isPipelinedOrHybridResultPartitionNewMode(partitionType, enabledTieredStorage)
                        ? DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_STREAM
                        : DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_BATCH;
        assertThat(effectiveMaxRequiredBuffers).isEqualTo(expectEffectiveMaxRequiredBuffers);
        enabledTieredStorage = true;
        expectEffectiveMaxRequiredBuffers =
                isPipelinedOrHybridResultPartitionNewMode(partitionType, enabledTieredStorage)
                        ? DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_STREAM
                        : DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_BATCH;
        effectiveMaxRequiredBuffers =
                getEffectiveMaxRequiredBuffersPerGate(
                        partitionType, emptyConfig, enabledTieredStorage);
        assertThat(effectiveMaxRequiredBuffers).isEqualTo(expectEffectiveMaxRequiredBuffers);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testDefaultMinBuffersPerGate(ResultPartitionType partitionType) {
        int numInputChannels = 499;
        int numExclusiveBuffers = 2;

        GateBuffersSpec gateBuffersSpec =
                createGateBuffersSpec(numInputChannels, partitionType, numExclusiveBuffers, null);
        int expectedBuffersPerGate = 999;
        int maxBuffersPerGate = 1006;
        checkBuffersInGate(
                gateBuffersSpec,
                numExclusiveBuffers,
                expectedBuffersPerGate,
                expectedBuffersPerGate,
                maxBuffersPerGate);

        TieredStorageConfiguration config =
                (new TieredStorageConfiguration.Builder().setMemoryDecouplingEnabled(true)).build();
        gateBuffersSpec =
                createGateBuffersSpec(numInputChannels, partitionType, numExclusiveBuffers, config);
        int minBuffersPerGate = partitionType.isHybridResultPartition() ? 2 : 999;
        checkBuffersInGate(
                gateBuffersSpec,
                numExclusiveBuffers,
                expectedBuffersPerGate,
                minBuffersPerGate,
                maxBuffersPerGate);
    }

    private static void checkBuffersInGate(
            GateBuffersSpec gateBuffersSpec,
            int numExclusivePerChannel,
            int expectedBuffersPerGate,
            int minBuffersPerGate,
            int maxBuffersPerGate) {
        assertThat(gateBuffersSpec.getEffectiveExclusiveBuffersPerChannel())
                .isEqualTo(numExclusivePerChannel);
        assertThat(gateBuffersSpec.getExpectedBuffersPerGate()).isEqualTo(expectedBuffersPerGate);
        assertThat(gateBuffersSpec.getMinBuffersPerGate()).isEqualTo(minBuffersPerGate);
        assertThat(gateBuffersSpec.getMaxBuffersPerGate()).isEqualTo(maxBuffersPerGate);
    }

    private static GateBuffersSpec createGateBuffersSpec(
            int numInputChannels, ResultPartitionType partitionType) {
        return createGateBuffersSpec(numInputChannels, partitionType, 2);
    }

    private static GateBuffersSpec createGateBuffersSpec(
            int numInputChannels,
            ResultPartitionType partitionType,
            int numExclusiveBuffersPerChannel) {
        return InputGateSpecUtils.createGateBuffersSpec(
                getMaxRequiredBuffersPerGate(partitionType),
                numExclusiveBuffersPerChannel,
                8,
                partitionType,
                numInputChannels,
                null);
    }

    private static GateBuffersSpec createGateBuffersSpec(
            int numInputChannels,
            ResultPartitionType partitionType,
            int numExclusiveBuffersPerChannel,
            @Nullable TieredStorageConfiguration config) {
        return InputGateSpecUtils.createGateBuffersSpec(
                getMaxRequiredBuffersPerGate(partitionType),
                numExclusiveBuffersPerChannel,
                8,
                partitionType,
                numInputChannels,
                config);
    }

    private static Optional<Integer> getMaxRequiredBuffersPerGate(
            ResultPartitionType partitionType) {
        return isPipelinedOrHybridResultPartition(partitionType)
                ? Optional.of(DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_STREAM)
                : Optional.of(DEFAULT_MAX_REQUIRED_BUFFERS_PER_GATE_FOR_BATCH);
    }

    private static boolean isPipelinedOrHybridResultPartition(ResultPartitionType partitionType) {
        return partitionType.isPipelinedOrPipelinedBoundedResultPartition()
                || partitionType.isHybridResultPartition();
    }

    private static boolean isPipelinedOrHybridResultPartitionNewMode(
            ResultPartitionType partitionType, Boolean enabledTieredStorage) {
        return partitionType.isPipelinedOrPipelinedBoundedResultPartition()
                || (partitionType.isHybridResultPartition() && !enabledTieredStorage);
    }
}
