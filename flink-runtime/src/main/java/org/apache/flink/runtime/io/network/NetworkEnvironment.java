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
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
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
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Network I/O components of each {@link TaskExecutor} instance. The network environment contains
 * the data structures that keep track of all intermediate results and all data exchanges.
 */
public class NetworkEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkEnvironment.class);

	private static final String METRIC_GROUP_NETWORK = "Network";
	private static final String METRIC_TOTAL_MEMORY_SEGMENT = "TotalMemorySegments";
	private static final String METRIC_AVAILABLE_MEMORY_SEGMENT = "AvailableMemorySegments";

	private static final String METRIC_OUTPUT_QUEUE_LENGTH = "outputQueueLength";
	private static final String METRIC_OUTPUT_POOL_USAGE = "outPoolUsage";
	private static final String METRIC_INPUT_QUEUE_LENGTH = "inputQueueLength";
	private static final String METRIC_INPUT_POOL_USAGE = "inPoolUsage";

	private final Object lock = new Object();

	private final NetworkEnvironmentConfiguration config;

	private final NetworkBufferPool networkBufferPool;

	private final ConnectionManager connectionManager;

	private final ResultPartitionManager resultPartitionManager;

	private final TaskEventPublisher taskEventPublisher;

	private final IOManager ioManager;

	private boolean isShutdown;

	public NetworkEnvironment(
			NetworkEnvironmentConfiguration config,
			TaskEventPublisher taskEventPublisher,
			MetricGroup metricGroup,
			IOManager ioManager) {
		this.config = checkNotNull(config);

		this.networkBufferPool = new NetworkBufferPool(config.numNetworkBuffers(), config.networkBufferSize());

		NettyConfig nettyConfig = config.nettyConfig();
		if (nettyConfig != null) {
			this.connectionManager = new NettyConnectionManager(nettyConfig, config.isCreditBased());
		} else {
			this.connectionManager = new LocalConnectionManager();
		}

		this.resultPartitionManager = new ResultPartitionManager();

		this.taskEventPublisher = checkNotNull(taskEventPublisher);

		registerNetworkMetrics(metricGroup, networkBufferPool);

		this.ioManager = checkNotNull(ioManager);

		isShutdown = false;
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

	public ResultPartitionManager getResultPartitionManager() {
		return resultPartitionManager;
	}

	public ConnectionManager getConnectionManager() {
		return connectionManager;
	}

	@VisibleForTesting
	public NetworkBufferPool getNetworkBufferPool() {
		return networkBufferPool;
	}

	public NetworkEnvironmentConfiguration getConfiguration() {
		return config;
	}

	// --------------------------------------------------------------------------------------------
	//  Task operations
	// --------------------------------------------------------------------------------------------

	public void registerTask(Task task) throws IOException {
		final ResultPartition[] producedPartitions = task.getProducedPartitions();

		synchronized (lock) {
			if (isShutdown) {
				throw new IllegalStateException("NetworkEnvironment is shut down");
			}

			for (final ResultPartition partition : producedPartitions) {
				setupPartition(partition);
			}

			// Setup the buffer pool for each buffer reader
			final SingleInputGate[] inputGates = task.getAllInputGates();
			for (SingleInputGate gate : inputGates) {
				setupInputGate(gate);
			}
		}
	}

	@VisibleForTesting
	public void setupPartition(ResultPartition partition) throws IOException {
		BufferPool bufferPool = null;

		try {
			int maxNumberOfMemorySegments = partition.getPartitionType().isBounded() ?
				partition.getNumberOfSubpartitions() * config.networkBuffersPerChannel() +
					config.floatingNetworkBuffersPerGate() : Integer.MAX_VALUE;
			// If the partition type is back pressure-free, we register with the buffer pool for
			// callbacks to release memory.
			bufferPool = networkBufferPool.createBufferPool(partition.getNumberOfSubpartitions(),
				maxNumberOfMemorySegments,
				partition.getPartitionType().hasBackPressure() ? Optional.empty() : Optional.of(partition));

			partition.registerBufferPool(bufferPool);

			resultPartitionManager.registerResultPartition(partition);
		} catch (Throwable t) {
			if (bufferPool != null) {
				bufferPool.lazyDestroy();
			}

			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException(t.getMessage(), t);
			}
		}
	}

	@VisibleForTesting
	public void setupInputGate(SingleInputGate gate) throws IOException {
		BufferPool bufferPool = null;
		int maxNumberOfMemorySegments;
		try {
			if (config.isCreditBased()) {
				maxNumberOfMemorySegments = gate.getConsumedPartitionType().isBounded() ?
					config.floatingNetworkBuffersPerGate() : Integer.MAX_VALUE;

				// assign exclusive buffers to input channels directly and use the rest for floating buffers
				gate.assignExclusiveSegments(networkBufferPool, config.networkBuffersPerChannel());
				bufferPool = networkBufferPool.createBufferPool(0, maxNumberOfMemorySegments);
			} else {
				maxNumberOfMemorySegments = gate.getConsumedPartitionType().isBounded() ?
					gate.getNumberOfInputChannels() * config.networkBuffersPerChannel() +
						config.floatingNetworkBuffersPerGate() : Integer.MAX_VALUE;

				bufferPool = networkBufferPool.createBufferPool(gate.getNumberOfInputChannels(),
					maxNumberOfMemorySegments);
			}
			gate.setBufferPool(bufferPool);
		} catch (Throwable t) {
			if (bufferPool != null) {
				bufferPool.lazyDestroy();
			}

			ExceptionUtils.rethrowIOException(t);
		}
	}

	/**
	 * Batch release intermediate result partitions.
	 *
	 * @param partitionIds partition ids to release
	 */
	public void releasePartitions(Collection<ResultPartitionID> partitionIds) {
		for (ResultPartitionID partitionId : partitionIds) {
			resultPartitionManager.releasePartition(partitionId, null);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Create Output Writers and Input Readers
	// --------------------------------------------------------------------------------------------

	public ResultPartition[] createResultPartitionWriters(
			String taskName,
			JobID jobId,
			ExecutionAttemptID executionId,
			TaskActions taskActions,
			ResultPartitionConsumableNotifier partitionConsumableNotifier,
			Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
			MetricGroup outputGroup,
			MetricGroup buffersGroup) {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown, "The NetworkEnvironment has already been shut down.");

			ResultPartition[] resultPartitions = new ResultPartition[resultPartitionDeploymentDescriptors.size()];
			int counter = 0;
			for (ResultPartitionDeploymentDescriptor rpdd : resultPartitionDeploymentDescriptors) {
				resultPartitions[counter++] = new ResultPartition(
					taskName,
					taskActions,
					jobId,
					new ResultPartitionID(rpdd.getPartitionId(), executionId),
					rpdd.getPartitionType(),
					rpdd.getNumberOfSubpartitions(),
					rpdd.getMaxParallelism(),
					resultPartitionManager,
					partitionConsumableNotifier,
					ioManager,
					rpdd.sendScheduleOrUpdateConsumersMessage());
			}

			registerOutputMetrics(outputGroup, buffersGroup, resultPartitions);
			return resultPartitions;
		}
	}

	public SingleInputGate[] createInputGates(
			String taskName,
			JobID jobId,
			TaskActions taskActions,
			Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
			MetricGroup parentGroup,
			MetricGroup inputGroup,
			MetricGroup buffersGroup,
			Counter numBytesInCounter) {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown, "The NetworkEnvironment has already been shut down.");

			InputChannelMetrics inputChannelMetrics = new InputChannelMetrics(parentGroup);
			SingleInputGate[] inputGates = new SingleInputGate[inputGateDeploymentDescriptors.size()];
			int counter = 0;
			for (InputGateDeploymentDescriptor igdd : inputGateDeploymentDescriptors) {
				inputGates[counter++] = SingleInputGate.create(
					taskName,
					jobId,
					igdd,
					this,
					taskEventPublisher,
					taskActions,
					inputChannelMetrics,
					numBytesInCounter);
			}

			registerInputMetrics(inputGroup, buffersGroup, inputGates);
			return inputGates;
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

	public void start() throws IOException {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown, "The NetworkEnvironment has already been shut down.");

			LOG.info("Starting the network environment and its components.");

			try {
				LOG.debug("Starting network connection manager");
				connectionManager.start(resultPartitionManager, taskEventPublisher);
			} catch (IOException t) {
				throw new IOException("Failed to instantiate network connection manager.", t);
			}
		}
	}

	/**
	 * Tries to shut down all network I/O components.
	 */
	public void shutdown() {
		synchronized (lock) {
			if (isShutdown) {
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

			isShutdown = true;
		}
	}

	public boolean isShutdown() {
		synchronized (lock) {
			return isShutdown;
		}
	}
}
