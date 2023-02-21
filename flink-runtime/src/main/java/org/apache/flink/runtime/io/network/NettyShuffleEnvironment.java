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

package org.apache.flink.runtime.io.network;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateID;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_INPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_OUTPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.createShuffleIOOwnerMetricGroup;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerDebloatingTaskMetrics;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerInputMetrics;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerOutputMetrics;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link ShuffleEnvironment} based on netty network communication, local
 * memory and disk files. The network environment contains the data structures that keep track of
 * all intermediate results and shuffle data exchanges.
 */
public class NettyShuffleEnvironment
        implements ShuffleEnvironment<ResultPartition, SingleInputGate> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleEnvironment.class);

    private final Object lock = new Object();

    private final ResourceID taskExecutorResourceId;

    private final NettyShuffleEnvironmentConfiguration config;

    private final NetworkBufferPool networkBufferPool;

    private final ConnectionManager connectionManager;

    private final ResultPartitionManager resultPartitionManager;

    private final FileChannelManager fileChannelManager;

    private final Map<InputGateID, Set<SingleInputGate>> inputGatesById;

    private final ResultPartitionFactory resultPartitionFactory;

    private final SingleInputGateFactory singleInputGateFactory;

    private final Executor ioExecutor;

    private final BatchShuffleReadBufferPool batchShuffleReadBufferPool;

    private final ScheduledExecutorService batchShuffleReadIOExecutor;

    private boolean isClosed;

    NettyShuffleEnvironment(
            ResourceID taskExecutorResourceId,
            NettyShuffleEnvironmentConfiguration config,
            NetworkBufferPool networkBufferPool,
            ConnectionManager connectionManager,
            ResultPartitionManager resultPartitionManager,
            FileChannelManager fileChannelManager,
            ResultPartitionFactory resultPartitionFactory,
            SingleInputGateFactory singleInputGateFactory,
            Executor ioExecutor,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor) {
        this.taskExecutorResourceId = taskExecutorResourceId;
        this.config = config;
        this.networkBufferPool = networkBufferPool;
        this.connectionManager = connectionManager;
        this.resultPartitionManager = resultPartitionManager;
        this.inputGatesById = new ConcurrentHashMap<>(10);
        this.fileChannelManager = fileChannelManager;
        this.resultPartitionFactory = resultPartitionFactory;
        this.singleInputGateFactory = singleInputGateFactory;
        this.ioExecutor = ioExecutor;
        this.batchShuffleReadBufferPool = batchShuffleReadBufferPool;
        this.batchShuffleReadIOExecutor = batchShuffleReadIOExecutor;
        this.isClosed = false;
    }

    // --------------------------------------------------------------------------------------------
    //  Properties
    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    public ResultPartitionManager getResultPartitionManager() {
        return resultPartitionManager;
    }

    @VisibleForTesting
    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @VisibleForTesting
    public NetworkBufferPool getNetworkBufferPool() {
        return networkBufferPool;
    }

    @VisibleForTesting
    public BatchShuffleReadBufferPool getBatchShuffleReadBufferPool() {
        return batchShuffleReadBufferPool;
    }

    @VisibleForTesting
    public ScheduledExecutorService getBatchShuffleReadIOExecutor() {
        return batchShuffleReadIOExecutor;
    }

    @VisibleForTesting
    public NettyShuffleEnvironmentConfiguration getConfiguration() {
        return config;
    }

    @VisibleForTesting
    public Optional<Collection<SingleInputGate>> getInputGate(InputGateID id) {
        return Optional.ofNullable(inputGatesById.get(id));
    }

    @Override
    public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {
        ioExecutor.execute(
                () -> {
                    for (ResultPartitionID partitionId : partitionIds) {
                        resultPartitionManager.releasePartition(partitionId, null);
                    }
                });
    }

    /**
     * Report unreleased partitions.
     *
     * @return collection of partitions which still occupy some resources locally on this task
     *     executor and have been not released yet.
     */
    @Override
    public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
        return resultPartitionManager.getUnreleasedPartitions();
    }

    // --------------------------------------------------------------------------------------------
    //  Create Output Writers and Input Readers
    // --------------------------------------------------------------------------------------------

    @Override
    public ShuffleIOOwnerContext createShuffleIOOwnerContext(
            String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup) {
        MetricGroup nettyGroup = createShuffleIOOwnerMetricGroup(checkNotNull(parentGroup));
        return new ShuffleIOOwnerContext(
                checkNotNull(ownerName),
                checkNotNull(executionAttemptID),
                parentGroup,
                nettyGroup.addGroup(METRIC_GROUP_OUTPUT),
                nettyGroup.addGroup(METRIC_GROUP_INPUT));
    }

    @Override
    public List<ResultPartition> createResultPartitionWriters(
            ShuffleIOOwnerContext ownerContext,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {
        synchronized (lock) {
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");

            ResultPartition[] resultPartitions =
                    new ResultPartition[resultPartitionDeploymentDescriptors.size()];
            for (int partitionIndex = 0;
                    partitionIndex < resultPartitions.length;
                    partitionIndex++) {
                resultPartitions[partitionIndex] =
                        resultPartitionFactory.create(
                                ownerContext.getOwnerName(),
                                partitionIndex,
                                resultPartitionDeploymentDescriptors.get(partitionIndex));
            }

            registerOutputMetrics(
                    config.isNetworkDetailedMetrics(),
                    ownerContext.getOutputGroup(),
                    resultPartitions);
            return Arrays.asList(resultPartitions);
        }
    }

    @Override
    public List<SingleInputGate> createInputGates(
            ShuffleIOOwnerContext ownerContext,
            PartitionProducerStateProvider partitionProducerStateProvider,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {
        synchronized (lock) {
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");

            MetricGroup networkInputGroup = ownerContext.getInputGroup();

            InputChannelMetrics inputChannelMetrics =
                    new InputChannelMetrics(networkInputGroup, ownerContext.getParentGroup());

            SingleInputGate[] inputGates =
                    new SingleInputGate[inputGateDeploymentDescriptors.size()];
            for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
                final InputGateDeploymentDescriptor igdd =
                        inputGateDeploymentDescriptors.get(gateIndex);
                SingleInputGate inputGate =
                        singleInputGateFactory.create(
                                ownerContext,
                                gateIndex,
                                igdd,
                                partitionProducerStateProvider,
                                inputChannelMetrics);
                InputGateID id =
                        new InputGateID(
                                igdd.getConsumedResultId(), ownerContext.getExecutionAttemptID());
                Set<SingleInputGate> inputGateSet =
                        inputGatesById.computeIfAbsent(
                                id, ignored -> ConcurrentHashMap.newKeySet());
                inputGateSet.add(inputGate);
                inputGatesById.put(id, inputGateSet);
                inputGate
                        .getCloseFuture()
                        .thenRun(
                                () ->
                                        inputGatesById.computeIfPresent(
                                                id,
                                                (key, value) -> {
                                                    value.remove(inputGate);
                                                    if (value.isEmpty()) {
                                                        return null;
                                                    }
                                                    return value;
                                                }));
                inputGates[gateIndex] = inputGate;
            }

            if (config.getDebloatConfiguration().isEnabled()) {
                registerDebloatingTaskMetrics(inputGates, ownerContext.getParentGroup());
            }

            registerInputMetrics(config.isNetworkDetailedMetrics(), networkInputGroup, inputGates);
            return Arrays.asList(inputGates);
        }
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
    public void registerLegacyNetworkMetrics(
            MetricGroup metricGroup,
            ResultPartitionWriter[] producedPartitions,
            InputGate[] inputGates) {
        NettyShuffleMetricFactory.registerLegacyNetworkMetrics(
                config.isNetworkDetailedMetrics(), metricGroup, producedPartitions, inputGates);
    }

    @Override
    public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo)
            throws IOException, InterruptedException {
        IntermediateDataSetID intermediateResultPartitionID =
                partitionInfo.getIntermediateDataSetID();
        InputGateID id = new InputGateID(intermediateResultPartitionID, consumerID);
        Set<SingleInputGate> inputGates = inputGatesById.get(id);
        if (inputGates == null || inputGates.isEmpty()) {
            return false;
        }

        ShuffleDescriptor shuffleDescriptor = partitionInfo.getShuffleDescriptor();
        checkArgument(
                shuffleDescriptor instanceof NettyShuffleDescriptor,
                "Tried to update unknown channel with unknown ShuffleDescriptor %s.",
                shuffleDescriptor.getClass().getName());
        for (SingleInputGate inputGate : inputGates) {
            inputGate.updateInputChannel(
                    taskExecutorResourceId, (NettyShuffleDescriptor) shuffleDescriptor);
        }
        return true;
    }

    /*
     * Starts the internal related components for network connection and communication.
     *
     * @return a port to connect to the task executor for shuffle data exchange, -1 if only local connection is possible.
     */
    @Override
    public int start() throws IOException {
        synchronized (lock) {
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");

            LOG.info("Starting the network environment and its components.");

            try {
                LOG.debug("Starting network connection manager");
                return connectionManager.start();
            } catch (IOException t) {
                throw new IOException("Failed to instantiate network connection manager.", t);
            }
        }
    }

    /** Tries to shut down all network I/O components. */
    @Override
    public void close() {
        synchronized (lock) {
            if (isClosed) {
                return;
            }

            LOG.info("Shutting down the network environment and its components.");

            // terminate all network connections
            try {
                LOG.debug("Shutting down network connection manager");
                connectionManager.shutdown();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down the network connection manager.", t);
            }

            // shutdown all intermediate results
            try {
                LOG.debug("Shutting down intermediate result partition manager");
                resultPartitionManager.shutdown();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down the result partition manager.", t);
            }

            // make sure that the global buffer pool re-acquires all buffers
            try {
                networkBufferPool.destroyAllBufferPools();
            } catch (Throwable t) {
                LOG.warn("Could not destroy all buffer pools.", t);
            }

            // destroy the buffer pool
            try {
                networkBufferPool.destroy();
            } catch (Throwable t) {
                LOG.warn("Network buffer pool did not shut down properly.", t);
            }

            // delete all the temp directories
            try {
                fileChannelManager.close();
            } catch (Throwable t) {
                LOG.warn("Cannot close the file channel manager properly.", t);
            }

            try {
                batchShuffleReadBufferPool.destroy();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down batch shuffle read buffer pool properly.", t);
            }

            try {
                batchShuffleReadIOExecutor.shutdown();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down batch shuffle read IO executor properly.", t);
            }

            isClosed = true;
        }
    }

    public boolean isClosed() {
        synchronized (lock) {
            return isClosed;
        }
    }
}
