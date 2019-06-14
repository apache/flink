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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputBufferPoolUsageGauge;
import org.apache.flink.runtime.io.network.metrics.InputBuffersGauge;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.metrics.InputGateMetrics;
import org.apache.flink.runtime.io.network.metrics.OutputBufferPoolUsageGauge;
import org.apache.flink.runtime.io.network.metrics.OutputBuffersGauge;
import org.apache.flink.runtime.io.network.metrics.ResultPartitionMetrics;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
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
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link ShuffleEnvironment} based on netty network communication, local memory and disk files.
 * The network environment contains the data structures that keep track of all intermediate results
 * and shuffle data exchanges.
 */
public class NettyShuffleEnvironment implements ShuffleEnvironment<ResultPartition, SingleInputGate> {

	private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleEnvironment.class);

	private static final String METRIC_GROUP_NETWORK = "Network";
	private static final String METRIC_TOTAL_MEMORY_SEGMENT = "TotalMemorySegments";
	private static final String METRIC_AVAILABLE_MEMORY_SEGMENT = "AvailableMemorySegments";

	private static final String METRIC_OUTPUT_QUEUE_LENGTH = "outputQueueLength";
	private static final String METRIC_OUTPUT_POOL_USAGE = "outPoolUsage";
	private static final String METRIC_INPUT_QUEUE_LENGTH = "inputQueueLength";
	private static final String METRIC_INPUT_POOL_USAGE = "inPoolUsage";

	private final Object lock = new Object();

	private final ResourceID taskExecutorLocation;

	private final NettyShuffleEnvironmentConfiguration config;

	private final NetworkBufferPool networkBufferPool;

	private final ConnectionManager connectionManager;

	private final ResultPartitionManager resultPartitionManager;

	private final Map<InputGateID, SingleInputGate> inputGatesById;

	private final ResultPartitionFactory resultPartitionFactory;

	private final SingleInputGateFactory singleInputGateFactory;

	private boolean isClosed;

	private NettyShuffleEnvironment(
			ResourceID taskExecutorLocation,
			NettyShuffleEnvironmentConfiguration config,
			NetworkBufferPool networkBufferPool,
			ConnectionManager connectionManager,
			ResultPartitionManager resultPartitionManager,
			ResultPartitionFactory resultPartitionFactory,
			SingleInputGateFactory singleInputGateFactory) {
		this.taskExecutorLocation = taskExecutorLocation;
		this.config = config;
		this.networkBufferPool = networkBufferPool;
		this.connectionManager = connectionManager;
		this.resultPartitionManager = resultPartitionManager;
		this.inputGatesById = new ConcurrentHashMap<>();
		this.resultPartitionFactory = resultPartitionFactory;
		this.singleInputGateFactory = singleInputGateFactory;
		this.isClosed = false;
	}

	public static NettyShuffleEnvironment create(
			NettyShuffleEnvironmentConfiguration config,
			ResourceID taskExecutorLocation,
			TaskEventPublisher taskEventPublisher,
			MetricGroup metricGroup,
			IOManager ioManager) {
		checkNotNull(taskExecutorLocation);
		checkNotNull(ioManager);
		checkNotNull(taskEventPublisher);
		checkNotNull(config);

		NettyConfig nettyConfig = config.nettyConfig();

		ResultPartitionManager resultPartitionManager = new ResultPartitionManager();

		ConnectionManager connectionManager = nettyConfig != null ?
			new NettyConnectionManager(resultPartitionManager, taskEventPublisher, nettyConfig, config.isCreditBased()) :
			new LocalConnectionManager();

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(
			config.numNetworkBuffers(),
			config.networkBufferSize(),
			config.networkBuffersPerChannel());

		registerNetworkMetrics(metricGroup, networkBufferPool);

		ResultPartitionFactory resultPartitionFactory = new ResultPartitionFactory(
			resultPartitionManager,
			ioManager,
			networkBufferPool,
			config.networkBuffersPerChannel(),
			config.floatingNetworkBuffersPerGate(),
			config.isForcePartitionReleaseOnConsumption());

		SingleInputGateFactory singleInputGateFactory = new SingleInputGateFactory(
			taskExecutorLocation,
			config,
			connectionManager,
			resultPartitionManager,
			taskEventPublisher,
			networkBufferPool);

		return new NettyShuffleEnvironment(
			taskExecutorLocation,
			config,
			networkBufferPool,
			connectionManager,
			resultPartitionManager,
			resultPartitionFactory,
			singleInputGateFactory);
	}

	private static void registerNetworkMetrics(MetricGroup metricGroup, NetworkBufferPool networkBufferPool) {
		checkNotNull(metricGroup);

		MetricGroup networkGroup = metricGroup.addGroup(METRIC_GROUP_NETWORK);
		networkGroup.<Integer, Gauge<Integer>>gauge(METRIC_TOTAL_MEMORY_SEGMENT,
			networkBufferPool::getTotalNumberOfMemorySegments);
		networkGroup.<Integer, Gauge<Integer>>gauge(METRIC_AVAILABLE_MEMORY_SEGMENT,
			networkBufferPool::getNumberOfAvailableMemorySegments);
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
	public NettyShuffleEnvironmentConfiguration getConfiguration() {
		return config;
	}

	@VisibleForTesting
	public Optional<InputGate> getInputGate(InputGateID id) {
		return Optional.ofNullable(inputGatesById.get(id));
	}

	@Override
	public void releasePartitions(Collection<ResultPartitionID> partitionIds) {
		for (ResultPartitionID partitionId : partitionIds) {
			resultPartitionManager.releasePartition(partitionId, null);
		}
	}

	/**
	 * Report unreleased partitions.
	 *
	 * @return collection of partitions which still occupy some resources locally on this task executor
	 * and have been not released yet.
	 */
	@Override
	public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
		return resultPartitionManager.getUnreleasedPartitions();
	}

	// --------------------------------------------------------------------------------------------
	//  Create Output Writers and Input Readers
	// --------------------------------------------------------------------------------------------

	@Override
	public Collection<ResultPartition> createResultPartitionWriters(
			String ownerName,
			ExecutionAttemptID executionAttemptID,
			Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
			MetricGroup outputGroup,
			MetricGroup buffersGroup) {
		synchronized (lock) {
			Preconditions.checkState(!isClosed, "The NettyShuffleEnvironment has already been shut down.");

			ResultPartition[] resultPartitions = new ResultPartition[resultPartitionDeploymentDescriptors.size()];
			int counter = 0;
			for (ResultPartitionDeploymentDescriptor rpdd : resultPartitionDeploymentDescriptors) {
				resultPartitions[counter++] = resultPartitionFactory.create(ownerName, executionAttemptID, rpdd);
			}

			registerOutputMetrics(outputGroup, buffersGroup, resultPartitions);
			return Arrays.asList(resultPartitions);
		}
	}

	@Override
	public Collection<SingleInputGate> createInputGates(
			String ownerName,
			ExecutionAttemptID executionAttemptID,
			PartitionProducerStateProvider partitionProducerStateProvider,
			Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
			MetricGroup parentGroup,
			MetricGroup inputGroup,
			MetricGroup buffersGroup) {
		synchronized (lock) {
			Preconditions.checkState(!isClosed, "The NettyShuffleEnvironment has already been shut down.");

			InputChannelMetrics inputChannelMetrics = new InputChannelMetrics(parentGroup);
			SingleInputGate[] inputGates = new SingleInputGate[inputGateDeploymentDescriptors.size()];
			int counter = 0;
			for (InputGateDeploymentDescriptor igdd : inputGateDeploymentDescriptors) {
				SingleInputGate inputGate = singleInputGateFactory.create(
					ownerName,
					igdd,
					partitionProducerStateProvider,
					inputChannelMetrics);
				InputGateID id = new InputGateID(igdd.getConsumedResultId(), executionAttemptID);
				inputGatesById.put(id, inputGate);
				inputGate.getCloseFuture().thenRun(() -> inputGatesById.remove(id));
				inputGates[counter++] = inputGate;
			}

			registerInputMetrics(inputGroup, buffersGroup, inputGates);
			return Arrays.asList(inputGates);
		}
	}

	private void registerOutputMetrics(MetricGroup outputGroup, MetricGroup buffersGroup, ResultPartition[] resultPartitions) {
		if (config.isNetworkDetailedMetrics()) {
			ResultPartitionMetrics.registerQueueLengthMetrics(outputGroup, resultPartitions);
		}
		buffersGroup.gauge(METRIC_OUTPUT_QUEUE_LENGTH, new OutputBuffersGauge(resultPartitions));
		buffersGroup.gauge(METRIC_OUTPUT_POOL_USAGE, new OutputBufferPoolUsageGauge(resultPartitions));
	}

	private void registerInputMetrics(MetricGroup inputGroup, MetricGroup buffersGroup, SingleInputGate[] inputGates) {
		if (config.isNetworkDetailedMetrics()) {
			InputGateMetrics.registerQueueLengthMetrics(inputGroup, inputGates);
		}
		buffersGroup.gauge(METRIC_INPUT_QUEUE_LENGTH, new InputBuffersGauge(inputGates));
		buffersGroup.gauge(METRIC_INPUT_POOL_USAGE, new InputBufferPoolUsageGauge(inputGates));
	}

	@Override
	public boolean updatePartitionInfo(
			ExecutionAttemptID consumerID,
			PartitionInfo partitionInfo) throws IOException, InterruptedException {
		IntermediateDataSetID intermediateResultPartitionID = partitionInfo.getIntermediateDataSetID();
		InputGateID id = new InputGateID(intermediateResultPartitionID, consumerID);
		SingleInputGate inputGate = inputGatesById.get(id);
		if (inputGate == null) {
			return false;
		}
		ShuffleDescriptor shuffleDescriptor = partitionInfo.getShuffleDescriptor();
		checkArgument(shuffleDescriptor instanceof NettyShuffleDescriptor,
			"Tried to update unknown channel with unknown ShuffleDescriptor %s.",
			shuffleDescriptor.getClass().getName());
		inputGate.updateInputChannel(taskExecutorLocation, (NettyShuffleDescriptor) shuffleDescriptor);
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
			Preconditions.checkState(!isClosed, "The NettyShuffleEnvironment has already been shut down.");

			LOG.info("Starting the network environment and its components.");

			try {
				LOG.debug("Starting network connection manager");
				return connectionManager.start();
			} catch (IOException t) {
				throw new IOException("Failed to instantiate network connection manager.", t);
			}
		}
	}

	/**
	 * Tries to shut down all network I/O components.
	 */
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
			}
			catch (Throwable t) {
				LOG.warn("Cannot shut down the network connection manager.", t);
			}

			// shutdown all intermediate results
			try {
				LOG.debug("Shutting down intermediate result partition manager");
				resultPartitionManager.shutdown();
			}
			catch (Throwable t) {
				LOG.warn("Cannot shut down the result partition manager.", t);
			}

			// make sure that the global buffer pool re-acquires all buffers
			networkBufferPool.destroyAllBufferPools();

			// destroy the buffer pool
			try {
				networkBufferPool.destroy();
			}
			catch (Throwable t) {
				LOG.warn("Network buffer pool did not shut down properly.", t);
			}

			isClosed = true;
		}
	}

	public boolean isClosed() {
		synchronized (lock) {
			return isClosed;
		}
	}

	public static NettyShuffleEnvironment fromShuffleContext(ShuffleEnvironmentContext shuffleEnvironmentContext) {
		NettyShuffleEnvironmentConfiguration networkConfig = NettyShuffleEnvironmentConfiguration.fromConfiguration(
			shuffleEnvironmentContext.getConfiguration(),
			shuffleEnvironmentContext.getMaxJvmHeapMemory(),
			shuffleEnvironmentContext.isLocalCommunicationOnly(),
			shuffleEnvironmentContext.getHostAddress());
		return create(
			networkConfig,
			shuffleEnvironmentContext.getLocation(),
			shuffleEnvironmentContext.getEventPublisher(),
			shuffleEnvironmentContext.getParentMetricGroup(),
			shuffleEnvironmentContext.getIOManager());
	}
}
