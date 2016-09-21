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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode.
 * It constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

	private final Object lock = new Object();

	private final Configuration configuration;

	private final ResourceID resourceID;

	private final RpcService rpcService;

	private final HighAvailabilityServices highAvailabilityServices;

	/** Executor used to run future callbacks */
	private final Executor executor;

	private final TaskExecutor taskManager;

	public TaskManagerRunner(
		Configuration configuration,
		ResourceID resourceID,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		Executor executor) throws Exception {

		this.configuration = Preconditions.checkNotNull(configuration);
		this.resourceID = Preconditions.checkNotNull(resourceID);
		this.rpcService = Preconditions.checkNotNull(rpcService);
		this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
		this.executor = rpcService.getExecutor();

		InetAddress remoteAddress = InetAddress.getByName(rpcService.getAddress());

		TaskManagerServicesConfiguration taskManagerServicesConfiguration = TaskManagerServicesConfiguration.fromConfiguration(
			configuration,
			remoteAddress,
			false);

		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(taskManagerServicesConfiguration, resourceID);

		TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);

		this.taskManager = new TaskExecutor(
			taskManagerConfiguration,
			taskManagerServices.getTaskManagerLocation(),
			rpcService,
			taskManagerServices.getMemoryManager(),
			taskManagerServices.getIOManager(),
			taskManagerServices.getNetworkEnvironment(),
			highAvailabilityServices,
			taskManagerServices.getMetricRegistry(),
			this);
	}

	// --------------------------------------------------------------------------------------------
	//  Lifecycle management
	// --------------------------------------------------------------------------------------------

	public void start() {
		taskManager.start();
	}

	public void shutDown(Throwable cause) {
		shutDownInternally();
	}

	protected void shutDownInternally() {
		synchronized(lock) {
			taskManager.shutDown();
		}
	}

	// --------------------------------------------------------------------------------------------
	//  FatalErrorHandler methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", exception);
		shutDown(exception);
	}

	// --------------------------------------------------------------------------------------------
	//  Static utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Create a RPC service for the task manager.
	 *
	 * @param configuration The configuration for the TaskManager.
	 * @param haServices to use for the task manager hostname retrieval
	 */
	public static RpcService createRpcService(
		final Configuration configuration,
		final HighAvailabilityServices haServices) throws Exception {

		checkNotNull(configuration);
		checkNotNull(haServices);

		String taskManagerHostname = configuration.getString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null);

		if (taskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: {}.", taskManagerHostname);
		} else {
			Time lookupTimeout = Time.milliseconds(AkkaUtils.getLookupTimeout(configuration).toMillis());

			InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
				haServices.getResourceManagerLeaderRetriever(),
				lookupTimeout);

			taskManagerHostname = taskManagerAddress.getHostName();

			LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
				taskManagerHostname, taskManagerAddress.getHostAddress());
		}

		final int rpcPort = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0);

		Preconditions.checkState(rpcPort < 0 || rpcPort >65535, "Invalid value for " +
				"'%s' (port for the TaskManager actor system) : %d - Leave config parameter empty or " +
				"use 0 to let the system choose port automatically.",
			ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, rpcPort);

		return RpcServiceUtils.createRpcService(taskManagerHostname, rpcPort, configuration);
	}
}
