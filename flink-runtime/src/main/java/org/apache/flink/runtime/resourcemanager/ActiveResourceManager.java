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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for {@link ResourceManager} implementations which contains some common variables and methods.
 */
public abstract class ActiveResourceManager <WorkerType extends ResourceIDRetrievable>
		extends ResourceManager<WorkerType> {

	/** The process environment variables. */
	protected final Map<String, String> env;

	protected final int numSlotsPerTaskManager;

	protected final TaskExecutorProcessSpec taskExecutorProcessSpec;

	protected final int defaultMemoryMB;

	protected final Collection<ResourceProfile> resourceProfilesPerWorker;

	/**
	 * The updated Flink configuration. The client uploaded configuration may be updated before passed on to
	 * {@link ResourceManager}. For example, {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}.
	 */
	protected final Configuration flinkConfig;

	/** Flink configuration uploaded by client. */
	protected final Configuration flinkClientConfig;

	public ActiveResourceManager(
			Configuration flinkConfig,
			Map<String, String> env,
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup) {
		super(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup);

		this.flinkConfig = flinkConfig;
		this.env = env;

		this.numSlotsPerTaskManager = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		double defaultCpus = getCpuCores(flinkConfig);
		this.taskExecutorProcessSpec = TaskExecutorProcessUtils
			.newProcessSpecBuilder(flinkConfig)
			.withCpuCores(defaultCpus)
			.build();
		this.defaultMemoryMB = taskExecutorProcessSpec.getTotalProcessMemorySize().getMebiBytes();

		this.resourceProfilesPerWorker = TaskExecutorProcessUtils
			.createDefaultWorkerSlotProfiles(taskExecutorProcessSpec, numSlotsPerTaskManager);

		// Load the flink config uploaded by flink client
		this.flinkClientConfig = loadClientConfiguration();
	}

	protected CompletableFuture<Void> getStopTerminationFutureOrCompletedExceptionally(@Nullable Throwable exception) {
		final CompletableFuture<Void> terminationFuture = super.onStop();

		if (exception != null) {
			return FutureUtils.completedExceptionally(new FlinkException(
				"Error while shutting down resource manager", exception));
		} else {
			return terminationFuture;
		}
	}

	protected abstract Configuration loadClientConfiguration();

	protected abstract double getCpuCores(final Configuration configuration);
}
