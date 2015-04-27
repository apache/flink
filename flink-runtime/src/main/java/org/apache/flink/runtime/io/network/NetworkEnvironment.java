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
import akka.dispatch.OnFailure;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.messages.JobManagerMessages.ScheduleOrUpdateConsumers;
import static org.apache.flink.runtime.messages.TaskMessages.FailTask;

/**
 * Network I/O components of each {@link TaskManager} instance. The network environment contains
 * the data structures that keep track of all intermediate results and all data exchanges.
 *
 * When initialized, the NetworkEnvironment will allocate the network buffer pool.
 * All other components (netty, intermediate result managers, ...) are only created once the
 * environment is "associated" with a TaskManager and JobManager. This happens as soon as the
 * TaskManager actor gets created and registers itself at the JobManager.
 */
public class NetworkEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkEnvironment.class);

	private final Object lock = new Object();

	private final NetworkEnvironmentConfiguration configuration;

	private final FiniteDuration jobManagerTimeout;

	private final NetworkBufferPool networkBufferPool;

	private ConnectionManager connectionManager;

	private ResultPartitionManager partitionManager;

	private TaskEventDispatcher taskEventDispatcher;

	private ResultPartitionConsumableNotifier partitionConsumableNotifier;

	private boolean isShutdown;


	/**
	 * Initializes all network I/O components.
	 */
	public NetworkEnvironment(FiniteDuration jobManagerTimeout,
								NetworkEnvironmentConfiguration config) throws IOException {

		this.configuration = checkNotNull(config);
		this.jobManagerTimeout = checkNotNull(jobManagerTimeout);

		// create the network buffers - this is the operation most likely to fail upon
		// mis-configuration, so we do this first
		try {
			networkBufferPool = new NetworkBufferPool(config.numNetworkBuffers(), config.networkBufferSize());
		}
		catch (Throwable t) {
			throw new IOException("Cannot allocate network buffer pool: " + t.getMessage(), t);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------

	public ResultPartitionManager getPartitionManager() {
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

	public IOMode getDefaultIOMode() {
		return configuration.ioMode();
	}

	public ResultPartitionConsumableNotifier getPartitionConsumableNotifier() {
		return partitionConsumableNotifier;
	}

	// --------------------------------------------------------------------------------------------
	//  Association / Disassociation with JobManager / TaskManager
	// --------------------------------------------------------------------------------------------

	public boolean isAssociated() {
		return partitionConsumableNotifier != null;
	}

	/**
	 * This associates the network environment with a TaskManager and JobManager.
	 * This will actually start the network components.
	 *
	 * @param jobManagerRef The JobManager actor reference.
	 * @param taskManagerRef The TaskManager actor reference.
	 *
	 * @throws IOException Thrown if the network subsystem (Netty) cannot be properly started.
	 */
	public void associateWithTaskManagerAndJobManager(ActorRef jobManagerRef, ActorRef taskManagerRef)
			throws IOException
	{
		checkNotNull(jobManagerRef);
		checkNotNull(taskManagerRef);

		synchronized (lock) {
			if (isShutdown) {
				throw new IllegalStateException("environment is shut down");
			}

			if (this.partitionConsumableNotifier == null &&
				this.partitionManager == null &&
				this.taskEventDispatcher == null &&
				this.connectionManager == null)
			{
				// good, not currently associated. start the individual components

				LOG.debug("Starting result partition manager and network connection manager");
				this.partitionManager = new ResultPartitionManager();
				this.taskEventDispatcher = new TaskEventDispatcher();
				this.partitionConsumableNotifier = new JobManagerResultPartitionConsumableNotifier(
													jobManagerRef, taskManagerRef, new Timeout(jobManagerTimeout));

				// -----  Network connections  -----
				final Option<NettyConfig> nettyConfig = configuration.nettyConfig();
				connectionManager = nettyConfig.isDefined() ? new NettyConnectionManager(nettyConfig.get())
															: new LocalConnectionManager();

				try {
					LOG.debug("Starting network connection manager");
					connectionManager.start(partitionManager, taskEventDispatcher, networkBufferPool);
				}
				catch (Throwable t) {
					throw new IOException("Failed to instantiate network connection manager: " + t.getMessage(), t);
				}
			}
			else {
				throw new IllegalStateException(
						"Network Environment is already associated with a JobManager/TaskManager");
			}
		}
	}

	public void disassociate() throws IOException {
		synchronized (lock) {
			if (!isAssociated()) {
				return;
			}

			LOG.debug("Disassociating NetworkEnvironment from TaskManager. Cleaning all intermediate results.");

			// terminate all network connections
			if (connectionManager != null) {
				try {
					LOG.debug("Shutting down network connection manager");
					connectionManager.shutdown();
					connectionManager = null;
				}
				catch (Throwable t) {
					throw new IOException("Cannot shutdown network connection manager", t);
				}
			}

			// shutdown all intermediate results
			if (partitionManager != null) {
				try {
					LOG.debug("Shutting down intermediate result partition manager");
					partitionManager.shutdown();
					partitionManager = null;
				}
				catch (Throwable t) {
					throw new IOException("Cannot shutdown partition manager", t);
				}
			}

			partitionConsumableNotifier = null;

			if (taskEventDispatcher != null) {
				taskEventDispatcher.clearAll();
				taskEventDispatcher = null;
			}

			// make sure that the global buffer pool re-acquires all buffers
			networkBufferPool.destroyAllBufferPools();
		}
	}



	// --------------------------------------------------------------------------------------------
	//  Task operations
	// --------------------------------------------------------------------------------------------

	public void registerTask(Task task) throws IOException {
		final ResultPartition[] producedPartitions = task.getProducedPartitions();
		final ResultPartitionWriter[] writers = task.getWriters();

		if (writers.length != producedPartitions.length) {
			throw new IllegalStateException("Unequal number of writers and partitions.");
		}

		synchronized (lock) {
			if (isShutdown) {
				throw new IllegalStateException("NetworkEnvironment is shut down");
			}
			if (!isAssociated()) {
				throw new IllegalStateException("NetworkEnvironment is not associated with a TaskManager");
			}

			for (int i = 0; i < producedPartitions.length; i++) {
				final ResultPartition partition = producedPartitions[i];
				final ResultPartitionWriter writer = writers[i];

				// Buffer pool for the partition
				BufferPool bufferPool = null;

				try {
					bufferPool = networkBufferPool.createBufferPool(partition.getNumberOfSubpartitions(), false);
					partition.registerBufferPool(bufferPool);

					partitionManager.registerResultPartition(partition);
				}
				catch (Throwable t) {
					if (bufferPool != null) {
						bufferPool.lazyDestroy();
					}

					if (t instanceof IOException) {
						throw (IOException) t;
					}
					else {
						throw new IOException(t.getMessage(), t);
					}
				}

				// Register writer with task event dispatcher
				taskEventDispatcher.registerWriterForIncomingTaskEvents(writer.getPartitionId(), writer);
			}

			// Setup the buffer pool for each buffer reader
			final SingleInputGate[] inputGates = task.getInputGates();

			for (SingleInputGate gate : inputGates) {
				BufferPool bufferPool = null;

				try {
					bufferPool = networkBufferPool.createBufferPool(gate.getNumberOfInputChannels(), false);
					gate.setBufferPool(bufferPool);
				}
				catch (Throwable t) {
					if (bufferPool != null) {
						bufferPool.lazyDestroy();
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
	}

	public void unregisterTask(Task task) {
		LOG.debug("Unregister task {} from network environment (state: {}).",
				task.getTaskNameWithSubtasks(), task.getExecutionState());

		final ExecutionAttemptID executionId = task.getExecutionId();

		synchronized (lock) {
			if (isShutdown || !isAssociated()) {
				// no need to do anything when we are not operational
				return;
			}

			if (task.isCanceledOrFailed()) {
				partitionManager.releasePartitionsProducedBy(executionId);
			}

			ResultPartitionWriter[] writers = task.getWriters();

			if (writers != null) {
				for (ResultPartitionWriter writer : task.getWriters()) {
					taskEventDispatcher.unregisterWriter(writer);
				}
			}

			final SingleInputGate[] inputGates = task.getInputGates();

			if (inputGates != null) {
				for (SingleInputGate gate : inputGates) {
					try {
						if (gate != null) {
							gate.releaseAllResources();
						}
					}
					catch (IOException e) {
						LOG.error("Error during release of reader resources: " + e.getMessage(), e);
					}
				}
			}
		}
	}

	public boolean hasReleasedAllResources() {
		String msg = String.format("Network buffer pool: %d missing memory segments. %d registered buffer pools. Connection manager: %d active connections. Task event dispatcher: %d registered writers.",
				networkBufferPool.getTotalNumberOfMemorySegments() - networkBufferPool.getNumberOfAvailableMemorySegments(),
				networkBufferPool.getNumberOfRegisteredBufferPools(), connectionManager.getNumberOfActiveConnections(),
				taskEventDispatcher.getNumberOfRegisteredWriters());

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
		synchronized (lock) {
			if (isShutdown) {
				return;
			}

			// shut down all connections and free all intermediate result partitions
			try {
				disassociate();
			}
			catch (Throwable t) {
				LOG.warn("Network services did not shut down properly: " + t.getMessage(), t);
			}

			// destroy the buffer pool
			try {
				networkBufferPool.destroy();
			}
			catch (Throwable t) {
				LOG.warn("Network buffer pool did not shut down properly: " + t.getMessage(), t);
			}

			isShutdown = true;
		}
	}

	public boolean isShutdown() {
		return isShutdown;
	}

	/**
	 * Notifies the job manager about consumable partitions.
	 */
	private static class JobManagerResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

		private final ActorRef jobManager;

		private final ActorRef taskManager;

		private final Timeout jobManagerMessageTimeout;

		public JobManagerResultPartitionConsumableNotifier(ActorRef jobManager,
															ActorRef taskManager,
															Timeout jobManagerMessageTimeout) {
			this.jobManager = jobManager;
			this.taskManager = taskManager;
			this.jobManagerMessageTimeout = jobManagerMessageTimeout;
		}

		@Override
		public void notifyPartitionConsumable(JobID jobId, final ResultPartitionID partitionId) {

			final ScheduleOrUpdateConsumers msg = new ScheduleOrUpdateConsumers(jobId, partitionId);

			Future<Object> futureResponse = Patterns.ask(jobManager, msg, jobManagerMessageTimeout);

			futureResponse.onFailure(new OnFailure() {
				@Override
				public void onFailure(Throwable failure) {
					LOG.error("Could not schedule or update consumers at the JobManager.", failure);

					// Fail task at the TaskManager
					FailTask failMsg = new FailTask(
							partitionId.getProducerId(),
							new RuntimeException("Could not notify JobManager to schedule or update consumers",
									failure));

					taskManager.tell(failMsg, ActorRef.noSender());
				}
			}, AkkaUtils.globalExecutionContext());
		}
	}
}
