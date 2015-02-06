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

import akka.actor.ActorRef;
import akka.util.Timeout;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionManager;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Network I/O components of each {@link TaskManager} instance.
 */
public class NetworkEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkEnvironment.class);

	private final ActorRef taskManager;

	private final ActorRef jobManager;

	private final FiniteDuration jobManagerTimeout;

	private final IntermediateResultPartitionManager partitionManager;

	private final TaskEventDispatcher taskEventDispatcher;

	private final NetworkBufferPool networkBufferPool;

	private final ConnectionManager connectionManager;

	private boolean isShutdown;

	/**
	 * Initializes all network I/O components.
	 */
	public NetworkEnvironment(ActorRef taskManager, ActorRef jobManager,
							FiniteDuration jobManagerTimeout,
							NetworkEnvironmentConfiguration config) throws IOException {
		this.taskManager = checkNotNull(taskManager);
		this.jobManager = checkNotNull(jobManager);
		this.jobManagerTimeout = checkNotNull(jobManagerTimeout);

		this.partitionManager = new IntermediateResultPartitionManager();
		this.taskEventDispatcher = new TaskEventDispatcher();

		// --------------------------------------------------------------------
		// Network buffers
		// --------------------------------------------------------------------
		try {
			networkBufferPool = new NetworkBufferPool(config.numNetworkBuffers(), config.networkBufferSize());
		}
		catch (Throwable t) {
			throw new IOException("Failed to instantiate network buffer pool: " + t.getMessage(), t);
		}

		// --------------------------------------------------------------------
		// Network connections
		// --------------------------------------------------------------------
		final Option<NettyConfig> nettyConfig = config.nettyConfig();

		connectionManager = nettyConfig.isDefined() ? new NettyConnectionManager(nettyConfig.get()) : new LocalConnectionManager();

		try {
			connectionManager.start(partitionManager, taskEventDispatcher);
		}
		catch (Throwable t) {
			throw new IOException("Failed to instantiate network connection manager: " + t.getMessage(), t);
		}
	}

	public ActorRef getTaskManager() {
		return taskManager;
	}

	public ActorRef getJobManager() {
		return jobManager;
	}

	public Timeout getJobManagerTimeout() {
		return new Timeout(jobManagerTimeout);
	}

	public void registerTask(Task task) throws IOException {
		final ExecutionAttemptID executionId = task.getExecutionId();

		final IntermediateResultPartition[] producedPartitions = task.getProducedPartitions();
		final BufferWriter[] writers = task.getWriters();

		if (writers.length != producedPartitions.length) {
			throw new IllegalStateException("Unequal number of writers and partitions.");
		}

		for (int i = 0; i < producedPartitions.length; i++) {
			final IntermediateResultPartition partition = producedPartitions[i];
			final BufferWriter writer = writers[i];

			// Buffer pool for the partition
			BufferPool bufferPool = null;

			try {
				bufferPool = networkBufferPool.createBufferPool(partition.getNumberOfQueues(), false);
				partition.setBufferPool(bufferPool);
				partitionManager.registerIntermediateResultPartition(partition);
			}
			catch (Throwable t) {
				if (bufferPool != null) {
					bufferPool.destroy();
				}

				if (t instanceof IOException) {
					throw (IOException) t;
				}
				else {
					throw new IOException(t.getMessage(), t);
				}
			}

			// Register writer with task event dispatcher
			taskEventDispatcher.registerWriterForIncomingTaskEvents(executionId, writer.getPartitionId(), writer);
		}

		// Setup the buffer pool for each buffer reader
		final BufferReader[] readers = task.getReaders();

		for (BufferReader reader : readers) {
			BufferPool bufferPool = null;

			try {
				bufferPool = networkBufferPool.createBufferPool(reader.getNumberOfInputChannels(), false);
				reader.setBufferPool(bufferPool);
			}
			catch (Throwable t) {
				if (bufferPool != null) {
					bufferPool.destroy();
				}

				if (t instanceof IOException) {
					throw (IOException) t;
				}
				else {
					throw new IOException(t.getMessage(), t);
				}
			}
		}
	}

	public void unregisterTask(Task task) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Unregistering task {} ({}) from network environment (state: {}).", task.getTaskNameWithSubtasks(), task.getExecutionId(), task.getExecutionState());
		}

		final ExecutionAttemptID executionId = task.getExecutionId();

		if (task.isCanceledOrFailed()) {
			partitionManager.failIntermediateResultPartitions(executionId);
		}

		taskEventDispatcher.unregisterWriters(executionId);

		final BufferReader[] readers = task.getReaders();

		if (readers != null) {
			for (BufferReader reader : readers) {
				try {
					if (reader != null) {
						reader.releaseAllResources();
					}
				}
				catch (IOException e) {
					LOG.error("Error during release of reader resources: " + e.getMessage(), e);
				}
			}
		}
	}

	public IntermediateResultPartitionManager getPartitionManager() {
		return partitionManager;
	}

	public TaskEventDispatcher getTaskEventDispatcher() {
		return taskEventDispatcher;
	}

	public ConnectionManager getConnectionManager() {
		return connectionManager;
	}

	public NetworkBufferPool getNetworkBufferPool() {
		return networkBufferPool;
	}

	public boolean hasReleasedAllResources() {
		String msg = String.format("Network buffer pool: %d missing memory segments. %d registered buffer pools. Connection manager: %d active connections. Task event dispatcher: %d registered writers.",
				networkBufferPool.getTotalNumberOfMemorySegments() - networkBufferPool.getNumberOfAvailableMemorySegments(), networkBufferPool.getNumberOfRegisteredBufferPools(), connectionManager.getNumberOfActiveConnections(), taskEventDispatcher.getNumberOfRegisteredWriters());

		boolean success = networkBufferPool.getTotalNumberOfMemorySegments() == networkBufferPool.getNumberOfAvailableMemorySegments() &&
				networkBufferPool.getNumberOfRegisteredBufferPools() == 0 &&
				connectionManager.getNumberOfActiveConnections() == 0 &&
				taskEventDispatcher.getNumberOfRegisteredWriters() == 0;

		if (success) {
			String successMsg = "Network environment did release all resources: " + msg;
			LOG.debug(successMsg);
		}
		else {
			String errMsg = "Network environment did *not* release all resources: " + msg;

			LOG.error(errMsg);
		}

		return success;
	}

	/**
	 * Tries to shut down all network I/O components.
	 */
	public void shutdown() {
		if (!isShutdown) {
			try {
				if (networkBufferPool != null) {
					networkBufferPool.destroy();
				}
			}
			catch (Throwable t) {
				LOG.warn("Network buffer pool did not shut down properly: " + t.getMessage(), t);
			}

			if (partitionManager != null) {
				try {
					partitionManager.shutdown();
				}
				catch (Throwable t) {
					LOG.warn("Partition manager did not shut down properly: " + t.getMessage(), t);
				}
			}

			try {
				if (connectionManager != null) {
					connectionManager.shutdown();
				}
			}
			catch (Throwable t) {
				LOG.warn("Network connection manager did not shut down properly: " + t.getMessage(), t);
			}

			isShutdown = true;
		}
	}

	public boolean isShutdown() {
		return isShutdown;
	}
}
