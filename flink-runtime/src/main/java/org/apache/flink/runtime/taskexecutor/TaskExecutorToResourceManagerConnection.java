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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The connection between a TaskExecutor and the ResourceManager.
 */
public class TaskExecutorToResourceManagerConnection
		extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

	private final RpcService rpcService;

	private final String taskManagerAddress;

	private final ResourceID taskManagerResourceId;

	private final SlotReport slotReport;

	private final int dataPort;

	private final HardwareDescription hardwareDescription;

	private final RegistrationConnectionListener<TaskExecutorRegistrationSuccess> registrationListener;

	private InstanceID registrationId;

	private ResourceID resourceManagerResourceId;

	public TaskExecutorToResourceManagerConnection(
			Logger log,
			RpcService rpcService,
			String taskManagerAddress,
			ResourceID taskManagerResourceId,
			SlotReport slotReport,
			int dataPort,
			HardwareDescription hardwareDescription,
			String resourceManagerAddress,
			ResourceManagerId resourceManagerId,
			Executor executor,
			RegistrationConnectionListener<TaskExecutorRegistrationSuccess> registrationListener) {

		super(log, resourceManagerAddress, resourceManagerId, executor);

		this.rpcService = Preconditions.checkNotNull(rpcService);
		this.taskManagerAddress = Preconditions.checkNotNull(taskManagerAddress);
		this.taskManagerResourceId = Preconditions.checkNotNull(taskManagerResourceId);
		this.slotReport = Preconditions.checkNotNull(slotReport);
		this.dataPort = dataPort;
		this.hardwareDescription = Preconditions.checkNotNull(hardwareDescription);
		this.registrationListener = Preconditions.checkNotNull(registrationListener);
	}


	@Override
	protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> generateRegistration() {
		return new TaskExecutorToResourceManagerConnection.ResourceManagerRegistration(
			log,
			rpcService,
			getTargetAddress(),
			getTargetLeaderId(),
			taskManagerAddress,
			taskManagerResourceId,
			slotReport,
			dataPort,
			hardwareDescription);
	}

	@Override
	protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
		log.info("Successful registration at resource manager {} under registration id {}.",
			getTargetAddress(), success.getRegistrationId());

		registrationId = success.getRegistrationId();
		resourceManagerResourceId = success.getResourceManagerId();
		registrationListener.onRegistrationSuccess(success);
	}

	@Override
	protected void onRegistrationFailure(Throwable failure) {
		log.info("Failed to register at resource manager {}.", getTargetAddress(), failure);

		registrationListener.onRegistrationFailure(failure);
	}

	/**
	 * Gets the ID under which the TaskExecutor is registered at the ResourceManager.
	 * This returns null until the registration is completed.
	 */
	public InstanceID getRegistrationId() {
		return registrationId;
	}

	/**
	 * Gets the unique id of ResourceManager, that is returned when registration success.
	 */
	public ResourceID getResourceManagerId() {
		return resourceManagerResourceId;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

		private final String taskExecutorAddress;
		
		private final ResourceID resourceID;

		private final SlotReport slotReport;

		private final int dataPort;

		private final HardwareDescription hardwareDescription;

		ResourceManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,
				ResourceManagerId resourceManagerId,
				String taskExecutorAddress,
				ResourceID resourceID,
				SlotReport slotReport,
				int dataPort,
				HardwareDescription hardwareDescription) {

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, resourceManagerId);
			this.taskExecutorAddress = checkNotNull(taskExecutorAddress);
			this.resourceID = checkNotNull(resourceID);
			this.slotReport = checkNotNull(slotReport);
			this.dataPort = dataPort;
			this.hardwareDescription = checkNotNull(hardwareDescription);
		}

		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway resourceManager, ResourceManagerId fencingToken, long timeoutMillis) throws Exception {

			Time timeout = Time.milliseconds(timeoutMillis);
			return resourceManager.registerTaskExecutor(
				taskExecutorAddress,
				resourceID,
				slotReport,
				dataPort,
				hardwareDescription,
				timeout);
		}
	}
}
