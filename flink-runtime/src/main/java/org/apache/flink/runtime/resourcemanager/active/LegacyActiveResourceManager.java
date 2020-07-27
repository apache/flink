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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for {@link ResourceManager} implementations which contains some common variables and methods.
 */
public abstract class LegacyActiveResourceManager<WorkerType extends ResourceIDRetrievable>
		extends ResourceManager<WorkerType> {

	/** The process environment variables. */
	protected final Map<String, String> env;

	/**
	 * The updated Flink configuration. The client uploaded configuration may be updated before passed on to
	 * {@link ResourceManager}. For example, {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}.
	 */
	protected final Configuration flinkConfig;

	/** Flink configuration uploaded by client. */
	protected final Configuration flinkClientConfig;

	private final PendingWorkerCounter requestedNotAllocatedWorkerCounter;
	private final PendingWorkerCounter requestedNotRegisteredWorkerCounter;

	/** Maps from worker's resource id to its resource spec. */
	private final Map<ResourceID, WorkerResourceSpec> allocatedNotRegisteredWorkerResourceSpecs;

	public LegacyActiveResourceManager(
			Configuration flinkConfig,
			Map<String, String> env,
			RpcService rpcService,
			ResourceID resourceId,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup) {
		super(
			rpcService,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			clusterPartitionTrackerFactory,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup,
			AkkaUtils.getTimeoutAsTime(flinkConfig));

		this.flinkConfig = flinkConfig;
		this.env = env;

		// Load the flink config uploaded by flink client
		this.flinkClientConfig = loadClientConfiguration();

		requestedNotAllocatedWorkerCounter = new PendingWorkerCounter();
		requestedNotRegisteredWorkerCounter = new PendingWorkerCounter();
		allocatedNotRegisteredWorkerResourceSpecs = new HashMap<>();
	}

	protected abstract Configuration loadClientConfiguration();

	@Override
	protected void onWorkerRegistered(WorkerType worker) {
		notifyAllocatedWorkerRegistered(worker.getResourceID());
	}

	protected int getNumRequestedNotAllocatedWorkers() {
		return requestedNotAllocatedWorkerCounter.getTotalNum();
	}

	protected int getNumRequestedNotAllocatedWorkersFor(WorkerResourceSpec workerResourceSpec) {
		return requestedNotAllocatedWorkerCounter.getNum(workerResourceSpec);
	}

	protected int getNumRequestedNotRegisteredWorkers() {
		return requestedNotRegisteredWorkerCounter.getTotalNum();
	}

	protected int getNumRequestedNotRegisteredWorkersFor(WorkerResourceSpec workerResourceSpec) {
		return requestedNotRegisteredWorkerCounter.getNum(workerResourceSpec);
	}

	/**
	 * Notify that a worker with the given resource spec has been requested.
	 * @param workerResourceSpec resource spec of the requested worker
	 * @return updated number of pending workers for the given resource spec
	 */
	protected PendingWorkerNums notifyNewWorkerRequested(WorkerResourceSpec workerResourceSpec) {
		return new PendingWorkerNums(
			requestedNotAllocatedWorkerCounter.increaseAndGet(workerResourceSpec),
			requestedNotRegisteredWorkerCounter.increaseAndGet(workerResourceSpec));
	}

	/**
	 * Notify that a worker with the given resource spec has been allocated.
	 * @param workerResourceSpec resource spec of the requested worker
	 * @param resourceID id of the allocated resource
	 * @return updated number of pending workers for the given resource spec
	 */
	protected PendingWorkerNums notifyNewWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID) {
		allocatedNotRegisteredWorkerResourceSpecs.put(resourceID, workerResourceSpec);
		return new PendingWorkerNums(
			requestedNotAllocatedWorkerCounter.decreaseAndGet(workerResourceSpec),
			requestedNotRegisteredWorkerCounter.getNum(workerResourceSpec));
	}

	/**
	 * Notify that allocation of a worker with the given resource spec has failed.
	 * @param workerResourceSpec resource spec of the requested worker
	 * @return updated number of pending workers for the given resource spec
	 */
	protected PendingWorkerNums notifyNewWorkerAllocationFailed(WorkerResourceSpec workerResourceSpec) {
		return new PendingWorkerNums(
			requestedNotAllocatedWorkerCounter.decreaseAndGet(workerResourceSpec),
			requestedNotRegisteredWorkerCounter.decreaseAndGet(workerResourceSpec));
	}

	/**
	 * Notify that a worker with the given resource spec has been registered.
	 * @param resourceID id of the registered worker resource
	 */
	private void notifyAllocatedWorkerRegistered(ResourceID resourceID) {
		WorkerResourceSpec workerResourceSpec = allocatedNotRegisteredWorkerResourceSpecs.remove(resourceID);
		if (workerResourceSpec == null) {
			// ignore workers from previous attempt
			return;
		}
		requestedNotRegisteredWorkerCounter.decreaseAndGet(workerResourceSpec);
	}

	/**
	 * Notify that a worker with the given resource spec has been stopped.
	 * @param resourceID id of the stopped worker resource
	 */
	protected void notifyAllocatedWorkerStopped(ResourceID resourceID) {
		WorkerResourceSpec workerResourceSpec = allocatedNotRegisteredWorkerResourceSpecs.remove(resourceID);
		if (workerResourceSpec == null) {
			// ignore already registered workers
			return;
		}
		requestedNotRegisteredWorkerCounter.decreaseAndGet(workerResourceSpec);
	}

	/**
	 * Number of workers pending for allocation/registration.
	 */
	protected static class PendingWorkerNums {

		private final int numNotAllocated;
		private final int numNotRegistered;

		private PendingWorkerNums(int numNotAllocated, int numNotRegistered) {
			this.numNotAllocated = numNotAllocated;
			this.numNotRegistered = numNotRegistered;
		}

		public int getNumNotAllocated() {
			return numNotAllocated;
		}

		public int getNumNotRegistered() {
			return numNotRegistered;
		}
	}
}
