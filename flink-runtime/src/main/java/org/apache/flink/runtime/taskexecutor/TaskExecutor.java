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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.SlotRequestRegistered;
import org.apache.flink.runtime.resourcemanager.SlotRequestReply;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link org.apache.flink.runtime.taskmanager.Task}.
 */
public class TaskExecutor extends RpcEndpoint<TaskExecutorGateway> {

	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

	/** The connection information of this task manager */
	private final TaskManagerLocation taskManagerLocation;

	/** The access to the leader election and retrieval services */
	private final HighAvailabilityServices haServices;

	/** The task manager configuration */
	private final TaskManagerConfiguration taskManagerConfiguration;

	/** The I/O manager component in the task manager */
	private final IOManager ioManager;

	/** The memory manager component in the task manager */
	private final MemoryManager memoryManager;

	/** The network component in the task manager */
	private final NetworkEnvironment networkEnvironment;

	/** The metric registry in the task manager */
	private final MetricRegistry metricRegistry;

	/** The number of slots in the task manager, should be 1 for YARN */
	private final int numberOfSlots;

	/** The fatal error handler to use in case of a fatal error */
	private final FatalErrorHandler fatalErrorHandler;

	// --------- resource manager --------

	private TaskExecutorToResourceManagerConnection resourceManagerConnection;

	// ------------------------------------------------------------------------

	public TaskExecutor(
		TaskManagerConfiguration taskManagerConfiguration,
		TaskManagerLocation taskManagerLocation,
		RpcService rpcService,
		MemoryManager memoryManager,
		IOManager ioManager,
		NetworkEnvironment networkEnvironment,
		HighAvailabilityServices haServices,
		MetricRegistry metricRegistry,
		FatalErrorHandler fatalErrorHandler) {

		super(rpcService);

		checkArgument(taskManagerConfiguration.getNumberSlots() > 0, "The number of slots has to be larger than 0.");

		this.taskManagerConfiguration = checkNotNull(taskManagerConfiguration);
		this.taskManagerLocation = checkNotNull(taskManagerLocation);
		this.memoryManager = checkNotNull(memoryManager);
		this.ioManager = checkNotNull(ioManager);
		this.networkEnvironment = checkNotNull(networkEnvironment);
		this.haServices = checkNotNull(haServices);
		this.metricRegistry = checkNotNull(metricRegistry);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

		this.numberOfSlots =  taskManagerConfiguration.getNumberSlots();
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@Override
	public void start() {
		super.start();

		// start by connecting to the ResourceManager
		try {
			haServices.getResourceManagerLeaderRetriever().start(new ResourceManagerLeaderListener());
		} catch (Exception e) {
			onFatalErrorAsync(e);
		}
	}

	// ------------------------------------------------------------------------
	//  RPC methods - ResourceManager related
	// ------------------------------------------------------------------------

	@RpcMethod
	public void notifyOfNewResourceManagerLeader(String newLeaderAddress, UUID newLeaderId) {
		if (resourceManagerConnection != null) {
			if (newLeaderAddress != null) {
				// the resource manager switched to a new leader
				log.info("ResourceManager leader changed from {} to {}. Registering at new leader.",
					resourceManagerConnection.getTargetAddress(), newLeaderAddress);
			}
			else {
				// address null means that the current leader is lost without a new leader being there, yet
				log.info("Current ResourceManager {} lost leader status. Waiting for new ResourceManager leader.",
					resourceManagerConnection.getTargetAddress());
			}

			// drop the current connection or connection attempt
			if (resourceManagerConnection != null) {
				resourceManagerConnection.close();
				resourceManagerConnection = null;
			}
		}

		// establish a connection to the new leader
		if (newLeaderAddress != null) {
			log.info("Attempting to register at ResourceManager {}", newLeaderAddress);
			resourceManagerConnection =
				new TaskExecutorToResourceManagerConnection(
					log,
					this,
					newLeaderAddress,
					newLeaderId,
					getMainThreadExecutor());
			resourceManagerConnection.start();
		}
	}

	/**
	 * Requests a slot from the TaskManager
	 *
	 * @param allocationID id for the request
	 * @param resourceManagerLeaderID current leader id of the ResourceManager
	 * @return answer to the slot request
	 */
	@RpcMethod
	public SlotRequestReply requestSlot(AllocationID allocationID, UUID resourceManagerLeaderID) {
		return new SlotRequestRegistered(allocationID);
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public ResourceID getResourceID() {
		return taskManagerLocation.getResourceID();
	}

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
	 * This method should be used when asynchronous threads want to notify the
	 * TaskExecutor of a fatal error.
	 *
	 * @param t The exception describing the fatal error
	 */
	void onFatalErrorAsync(final Throwable t) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				onFatalError(t);
			}
		});
	}

	/**
	 * Notifies the TaskExecutor that a fatal error has occurred and it cannot proceed.
	 * This method must only be called from within the TaskExecutor's main thread.
	 *
	 * @param t The exception describing the fatal error
	 */
	void onFatalError(Throwable t) {
		// to be determined, probably delegate to a fatal error handler that 
		// would either log (mini cluster) ot kill the process (yarn, mesos, ...)
		fatalErrorHandler.onFatalError(t);
	}

	// ------------------------------------------------------------------------
	//  Access to fields for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	TaskExecutorToResourceManagerConnection getResourceManagerConnection() {
		return resourceManagerConnection;
	}

	// ------------------------------------------------------------------------
	//  Utility classes
	// ------------------------------------------------------------------------

	/**
	 * The listener for leader changes of the resource manager
	 */
	private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
			getSelf().notifyOfNewResourceManagerLeader(leaderAddress, leaderSessionID);
		}

		@Override
		public void handleError(Exception exception) {
			onFatalErrorAsync(exception);
		}
	}

}
