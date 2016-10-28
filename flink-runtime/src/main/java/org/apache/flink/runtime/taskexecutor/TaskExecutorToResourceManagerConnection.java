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
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.concurrent.Future;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The connection between a TaskExecutor and the ResourceManager.
 */
public class TaskExecutorToResourceManagerConnection
		extends RegisteredRpcConnection<ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

	private final RpcService rpcService;

	private final String taskManagerAddress;

	private final ResourceID taskManagerResourceId;

	private final SlotReport slotReport;

	private final FatalErrorHandler fatalErrorHandler;

	private InstanceID registrationId;

	public TaskExecutorToResourceManagerConnection(
			Logger log,
			RpcService rpcService,
			String taskManagerAddress,
			ResourceID taskManagerResourceId,
			SlotReport slotReport,
			String resourceManagerAddress,
			UUID resourceManagerLeaderId,
			Executor executor,
			FatalErrorHandler fatalErrorHandler) {

		super(log, resourceManagerAddress, resourceManagerLeaderId, executor);

		this.rpcService = Preconditions.checkNotNull(rpcService);
		this.taskManagerAddress = Preconditions.checkNotNull(taskManagerAddress);
		this.taskManagerResourceId = Preconditions.checkNotNull(taskManagerResourceId);
		this.slotReport = Preconditions.checkNotNull(slotReport);
		this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);
	}


	@Override
	protected RetryingRegistration<ResourceManagerGateway, TaskExecutorRegistrationSuccess> generateRegistration() {
		return new TaskExecutorToResourceManagerConnection.ResourceManagerRegistration(
			log,
			rpcService,
			getTargetAddress(),
			getTargetLeaderId(),
			taskManagerAddress,
			taskManagerResourceId,
			slotReport);
	}

	@Override
	protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
		log.info("Successful registration at resource manager {} under registration id {}.",
			getTargetAddress(), success.getRegistrationId());

		registrationId = success.getRegistrationId();
	}

	@Override
	protected void onRegistrationFailure(Throwable failure) {
		log.info("Failed to register at resource manager {}.", getTargetAddress(), failure);

		fatalErrorHandler.onFatalError(failure);
	}

	/**
	 * Gets the ID under which the TaskExecutor is registered at the ResourceManager.
	 * This returns null until the registration is completed.
	 */
	public InstanceID getRegistrationId() {
		return registrationId;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

		private final String taskExecutorAddress;
		
		private final ResourceID resourceID;

		private final SlotReport slotReport;

		ResourceManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,
				UUID leaderId,
				String taskExecutorAddress,
				ResourceID resourceID,
				SlotReport slotReport) {

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, leaderId);
			this.taskExecutorAddress = checkNotNull(taskExecutorAddress);
			this.resourceID = checkNotNull(resourceID);
			this.slotReport = checkNotNull(slotReport);
		}

		@Override
		protected Future<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway resourceManager, UUID leaderId, long timeoutMillis) throws Exception {

			Time timeout = Time.milliseconds(timeoutMillis);
			return resourceManager.registerTaskExecutor(leaderId, taskExecutorAddress, resourceID, slotReport, timeout);
		}
	}
}
