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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.Constants;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * The entry point for running a TaskManager in a Kubernetes container.
 */
public class KubernetesTaskExecutorRunner {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesTaskExecutorRunner.class);

	/** The process environment variables. */
	private static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the yarn task executor runner failed. */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the YARN task executor runner.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "Kubernetes TaskExecutor runner", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		run(args);
	}

	/**
	 * The instance entry point for the Kubernetes task executor. Obtains user group information and calls
	 * the main work method {@link TaskManagerRunner#runTaskManager(Configuration, ResourceID)}  as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 */
	private static void run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			final Configuration configuration = GlobalConfiguration.loadConfiguration();

			// tell akka to die in case of an error
			configuration.setBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, true);

			// Infer the resource identifier from the environment variable
			String containerId = ENV.get(Constants.ENV_FLINK_CONTAINER_ID);
			Preconditions.checkArgument(containerId != null,
				"ContainerId variable %s not set", Constants.ENV_FLINK_CONTAINER_ID);
			LOG.info("ResourceID assigned for this container: {}", containerId);

			if (ENV.containsKey(Constants.ENV_TM_NUM_TASK_SLOT)) {
				int numSlots = Integer.valueOf(ENV.get(Constants.ENV_TM_NUM_TASK_SLOT));
				configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlots);
				LOG.info("Num slots for this container: {}", numSlots);
			}
			if (ENV.containsKey(Constants.ENV_TM_NUM_TASK_SLOT)) {
				String resourceProfile = ENV.get(Constants.ENV_TM_RESOURCE_PROFILE_KEY);
				configuration.setString(TaskManagerOptions.TASK_MANAGER_RESOURCE_PROFILE_KEY, resourceProfile);
				LOG.info("Resource profile for this container: {}", resourceProfile);
			}
			if (ENV.containsKey(Constants.ENV_TM_MANAGED_MEMORY_SIZE)) {
				long managedMemorySize = Long.valueOf(ENV.get(Constants.ENV_TM_MANAGED_MEMORY_SIZE));
				configuration.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, managedMemorySize);
			}
			if (ENV.containsKey(Constants.ENV_TM_FLOATING_MANAGED_MEMORY_SIZE)) {
				int floatingManagedMemory = Integer.valueOf(ENV.get(Constants.ENV_TM_FLOATING_MANAGED_MEMORY_SIZE));
				configuration.setInteger(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE.key(), floatingManagedMemory);
			}
			if (ENV.containsKey(Constants.ENV_TM_PROCESS_NETTY_MEMORY)) {
				int processNettyMemory = Integer.valueOf(ENV.get(Constants.ENV_TM_PROCESS_NETTY_MEMORY));
				configuration.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY, processNettyMemory);
			}
			if (ENV.containsKey(Constants.ENV_TM_NETWORK_BUFFERS_MEMORY_MIN)) {
				long networkBufMemoryMin = Long.valueOf(ENV.get(Constants.ENV_TM_NETWORK_BUFFERS_MEMORY_MIN));
				configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, networkBufMemoryMin);
			}
			if (ENV.containsKey(Constants.ENV_TM_NETWORK_BUFFERS_MEMORY_MAX)) {
				long networkBufMemoryMax = Long.valueOf(ENV.get(Constants.ENV_TM_NETWORK_BUFFERS_MEMORY_MAX));
				configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, networkBufMemoryMax);
			}

			SecurityConfiguration sc = new SecurityConfiguration(configuration);
			SecurityUtils.install(sc);
			SecurityUtils.getInstalledContext().runSecured(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					TaskManagerRunner.runTaskManager(configuration, new ResourceID(containerId));
					return null;
				}
			});
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("Kubernetes TaskManager initialization failed.", t);
			System.exit(INIT_ERROR_EXIT_CODE);
		}
	}
}
