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
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.RpcService;

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

	private final int dataPort;

	private final HardwareDescription hardwareDescription;

	private final RegistrationConnectionListener<TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess> registrationListener;

	public TaskExecutorToResourceManagerConnection(
			Logger log,
			RpcService rpcService,
			String taskManagerAddress,
			ResourceID taskManagerResourceId,
			int dataPort,
			HardwareDescription hardwareDescription,
			String resourceManagerAddress,
			ResourceManagerId resourceManagerId,
			Executor executor,
			RegistrationConnectionListener<TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess> registrationListener) {

		super(log, resourceManagerAddress, resourceManagerId, executor);

		this.rpcService = checkNotNull(rpcService);
		this.taskManagerAddress = checkNotNull(taskManagerAddress);
		this.taskManagerResourceId = checkNotNull(taskManagerResourceId);
		this.dataPort = dataPort;
		this.hardwareDescription = checkNotNull(hardwareDescription);
		this.registrationListener = checkNotNull(registrationListener);
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
			dataPort,
			hardwareDescription);
	}

	@Override
	protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
		log.info("Successful registration at resource manager {} under registration id {}.",
			getTargetAddress(), success.getRegistrationId());

		registrationListener.onRegistrationSuccess(this, success);
	}

	@Override
	protected void onRegistrationFailure(Throwable failure) {
		log.info("Failed to register at resource manager {}.", getTargetAddress(), failure);

		registrationListener.onRegistrationFailure(failure);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

		private final String taskExecutorAddress;

		private final ResourceID resourceID;

		private final int dataPort;

		private final HardwareDescription hardwareDescription;

		ResourceManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,
				ResourceManagerId resourceManagerId,
				String taskExecutorAddress,
				ResourceID resourceID,
				int dataPort,
				HardwareDescription hardwareDescription) {

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, resourceManagerId);
			this.taskExecutorAddress = checkNotNull(taskExecutorAddress);
			this.resourceID = checkNotNull(resourceID);
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
				dataPort,
				hardwareDescription,
				timeout);
		}
	}
}
