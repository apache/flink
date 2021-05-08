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

package org.apache.flink.runtime.io.network.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory for netty shuffle service metrics. */
public class NettyShuffleMetricFactory {

    // deprecated metric groups

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    private static final String METRIC_GROUP_NETWORK_DEPRECATED = "Network";

    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    private static final String METRIC_GROUP_BUFFERS_DEPRECATED = "buffers";

    // shuffle environment level metrics: Shuffle.Netty.*

    private static final String METRIC_TOTAL_MEMORY_SEGMENT = "TotalMemorySegments";
    private static final String METRIC_TOTAL_MEMORY = "TotalMemory";

    private static final String METRIC_AVAILABLE_MEMORY_SEGMENT = "AvailableMemorySegments";
    private static final String METRIC_AVAILABLE_MEMORY = "AvailableMemory";

    private static final String METRIC_USED_MEMORY_SEGMENT = "UsedMemorySegments";
    private static final String METRIC_USED_MEMORY = "UsedMemory";

    // task level metric group structure: Shuffle.Netty.<Input|Output>.Buffers

    private static final String METRIC_GROUP_SHUFFLE = "Shuffle";
    private static final String METRIC_GROUP_NETTY = "Netty";
    public static final String METRIC_GROUP_OUTPUT = "Output";
    public static final String METRIC_GROUP_INPUT = "Input";
    private static final String METRIC_GROUP_BUFFERS = "Buffers";

    // task level output metrics: Shuffle.Netty.Output.*

    private static final String METRIC_OUTPUT_QUEUE_LENGTH = "outputQueueLength";
    private static final String METRIC_OUTPUT_POOL_USAGE = "outPoolUsage";

    // task level input metrics: Shuffle.Netty.Input.*

    private static final String METRIC_INPUT_QUEUE_LENGTH = "inputQueueLength";
    private static final String METRIC_INPUT_POOL_USAGE = "inPoolUsage";
    private static final String METRIC_INPUT_FLOATING_BUFFERS_USAGE = "inputFloatingBuffersUsage";
    private static final String METRIC_INPUT_EXCLUSIVE_BUFFERS_USAGE = "inputExclusiveBuffersUsage";

    private NettyShuffleMetricFactory() {}

    public static void registerShuffleMetrics(
            MetricGroup metricGroup, NetworkBufferPool networkBufferPool) {
        checkNotNull(metricGroup);
        checkNotNull(networkBufferPool);

        //noinspection deprecation
        internalRegisterDeprecatedNetworkMetrics(metricGroup, networkBufferPool);
        internalRegisterShuffleMetrics(metricGroup, networkBufferPool);
    }

    @Deprecated
    private static void internalRegisterDeprecatedNetworkMetrics(
            MetricGroup parentMetricGroup, NetworkBufferPool networkBufferPool) {
        MetricGroup networkGroup = parentMetricGroup.addGroup(METRIC_GROUP_NETWORK_DEPRECATED);

        networkGroup.gauge(
                METRIC_TOTAL_MEMORY_SEGMENT, networkBufferPool::getTotalNumberOfMemorySegments);
        networkGroup.gauge(
                METRIC_AVAILABLE_MEMORY_SEGMENT,
                networkBufferPool::getNumberOfAvailableMemorySegments);
    }

    private static void internalRegisterShuffleMetrics(
            MetricGroup parentMetricGroup, NetworkBufferPool networkBufferPool) {
        MetricGroup shuffleGroup = parentMetricGroup.addGroup(METRIC_GROUP_SHUFFLE);
        MetricGroup networkGroup = shuffleGroup.addGroup(METRIC_GROUP_NETTY);

        networkGroup.gauge(
                METRIC_TOTAL_MEMORY_SEGMENT, networkBufferPool::getTotalNumberOfMemorySegments);
        networkGroup.gauge(METRIC_TOTAL_MEMORY, networkBufferPool::getTotalMemory);

        networkGroup.gauge(
                METRIC_AVAILABLE_MEMORY_SEGMENT,
                networkBufferPool::getNumberOfAvailableMemorySegments);
        networkGroup.gauge(METRIC_AVAILABLE_MEMORY, networkBufferPool::getAvailableMemory);

        networkGroup.gauge(
                METRIC_USED_MEMORY_SEGMENT, networkBufferPool::getNumberOfUsedMemorySegments);
        networkGroup.gauge(METRIC_USED_MEMORY, networkBufferPool::getUsedMemory);
    }

    public static MetricGroup createShuffleIOOwnerMetricGroup(MetricGroup parentGroup) {
        return parentGroup.addGroup(METRIC_GROUP_SHUFFLE).addGroup(METRIC_GROUP_NETTY);
    }

    /**
     * Registers legacy network metric groups before shuffle service refactoring.
     *
     * <p>Registers legacy metric groups if shuffle service implementation is original default one.
     *
     * @deprecated should be removed in future
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public static void registerLegacyNetworkMetrics(
            boolean isDetailedMetrics,
            MetricGroup metricGroup,
            ResultPartitionWriter[] producedPartitions,
            InputGate[] inputGates) {
        checkNotNull(metricGroup);
        checkNotNull(producedPartitions);
        checkNotNull(inputGates);

        // add metrics for buffers
        final MetricGroup buffersGroup = metricGroup.addGroup(METRIC_GROUP_BUFFERS_DEPRECATED);

        // similar to MetricUtils.instantiateNetworkMetrics() but inside this IOMetricGroup
        // (metricGroup)
        final MetricGroup networkGroup = metricGroup.addGroup(METRIC_GROUP_NETWORK_DEPRECATED);
        final MetricGroup outputGroup = networkGroup.addGroup(METRIC_GROUP_OUTPUT);
        final MetricGroup inputGroup = networkGroup.addGroup(METRIC_GROUP_INPUT);

        ResultPartition[] resultPartitions =
                Arrays.copyOf(
                        producedPartitions, producedPartitions.length, ResultPartition[].class);
        registerOutputMetrics(isDetailedMetrics, outputGroup, buffersGroup, resultPartitions);

        SingleInputGate[] singleInputGates =
                Arrays.copyOf(inputGates, inputGates.length, SingleInputGate[].class);
        registerInputMetrics(isDetailedMetrics, inputGroup, buffersGroup, singleInputGates);
    }

    public static void registerOutputMetrics(
            boolean isDetailedMetrics,
            MetricGroup outputGroup,
            ResultPartition[] resultPartitions) {
        registerOutputMetrics(
                isDetailedMetrics,
                outputGroup,
                outputGroup.addGroup(METRIC_GROUP_BUFFERS),
                resultPartitions);
    }

    private static void registerOutputMetrics(
            boolean isDetailedMetrics,
            MetricGroup outputGroup,
            MetricGroup buffersGroup,
            ResultPartition[] resultPartitions) {
        if (isDetailedMetrics) {
            ResultPartitionMetrics.registerQueueLengthMetrics(outputGroup, resultPartitions);
        }
        buffersGroup.gauge(METRIC_OUTPUT_QUEUE_LENGTH, new OutputBuffersGauge(resultPartitions));
        buffersGroup.gauge(
                METRIC_OUTPUT_POOL_USAGE, new OutputBufferPoolUsageGauge(resultPartitions));
    }

    public static void registerInputMetrics(
            boolean isDetailedMetrics, MetricGroup inputGroup, SingleInputGate[] inputGates) {
        registerInputMetrics(
                isDetailedMetrics,
                inputGroup,
                inputGroup.addGroup(METRIC_GROUP_BUFFERS),
                inputGates);
    }

    private static void registerInputMetrics(
            boolean isDetailedMetrics,
            MetricGroup inputGroup,
            MetricGroup buffersGroup,
            SingleInputGate[] inputGates) {
        if (isDetailedMetrics) {
            InputGateMetrics.registerQueueLengthMetrics(inputGroup, inputGates);
        }

        buffersGroup.gauge(METRIC_INPUT_QUEUE_LENGTH, new InputBuffersGauge(inputGates));

        FloatingBuffersUsageGauge floatingBuffersUsageGauge =
                new FloatingBuffersUsageGauge(inputGates);
        ExclusiveBuffersUsageGauge exclusiveBuffersUsageGauge =
                new ExclusiveBuffersUsageGauge(inputGates);
        CreditBasedInputBuffersUsageGauge creditBasedInputBuffersUsageGauge =
                new CreditBasedInputBuffersUsageGauge(
                        floatingBuffersUsageGauge, exclusiveBuffersUsageGauge, inputGates);
        buffersGroup.gauge(METRIC_INPUT_EXCLUSIVE_BUFFERS_USAGE, exclusiveBuffersUsageGauge);
        buffersGroup.gauge(METRIC_INPUT_FLOATING_BUFFERS_USAGE, floatingBuffersUsageGauge);
        buffersGroup.gauge(METRIC_INPUT_POOL_USAGE, creditBasedInputBuffersUsageGauge);
    }
}
