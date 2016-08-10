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

package org.apache.flink.runtime.rpc.taskexecutor;

import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.registration.RetryingRegistration;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;

import org.slf4j.Logger;

import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class TaskExecutorToResourceManagerConnection {

	/** the logger for all log messages of this class */
	private final Logger log;

	/** the TaskExecutor whose connection to the ResourceManager this represents */
	private final TaskExecutor taskExecutor;

	private final UUID resourceManagerLeaderId;

	private final String resourceManagerAddress;

	private ResourceManagerRegistration pendingRegistration;

	private ResourceManagerGateway registeredResourceManager;

	private InstanceID registrationId;

	/** flag indicating that the connection is closed */
	private volatile boolean closed;


	public TaskExecutorToResourceManagerConnection(
			Logger log,
			TaskExecutor taskExecutor,
			String resourceManagerAddress,
			UUID resourceManagerLeaderId) {

		this.log = checkNotNull(log);
		this.taskExecutor = checkNotNull(taskExecutor);
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress);
		this.resourceManagerLeaderId = checkNotNull(resourceManagerLeaderId);
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public void start() {
		checkState(!closed, "The connection is already closed");
		checkState(!isRegistered() && pendingRegistration == null, "The connection is already started");

		ResourceManagerRegistration registration = new ResourceManagerRegistration(
				log, taskExecutor.getRpcService(),
				resourceManagerAddress, resourceManagerLeaderId,
				taskExecutor.getAddress(), taskExecutor.getResourceID());

		Future<Tuple2<ResourceManagerGateway, TaskExecutorRegistrationSuccess>> future = registration.getFuture();
		
		future.onSuccess(new OnSuccess<Tuple2<ResourceManagerGateway, TaskExecutorRegistrationSuccess>>() {
			@Override
			public void onSuccess(Tuple2<ResourceManagerGateway, TaskExecutorRegistrationSuccess> result) {
				registeredResourceManager = result.f0;
				registrationId = result.f1.getRegistrationId();
			}
		}, taskExecutor.getMainThreadExecutionContext());
		
		// this future should only ever fail if there is a bug, not if the registration is declined
		future.onFailure(new OnFailure() {
			@Override
			public void onFailure(Throwable failure) {
				taskExecutor.onFatalError(failure);
			}
		}, taskExecutor.getMainThreadExecutionContext());
	}

	public void close() {
		closed = true;

		// make sure we do not keep re-trying forever
		if (pendingRegistration != null) {
			pendingRegistration.cancel();
		}
	}

	public boolean isClosed() {
		return closed;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public UUID getResourceManagerLeaderId() {
		return resourceManagerLeaderId;
	}

	public String getResourceManagerAddress() {
		return resourceManagerAddress;
	}

	/**
	 * Gets the ResourceManagerGateway. This returns null until the registration is completed.
	 */
	public ResourceManagerGateway getResourceManager() {
		return registeredResourceManager;
	}

	/**
	 * Gets the ID under which the TaskExecutor is registered at the ResourceManager.
	 * This returns null until the registration is completed.
	 */
	public InstanceID getRegistrationId() {
		return registrationId;
	}

	public boolean isRegistered() {
		return registeredResourceManager != null;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Connection to ResourceManager %s (leaderId=%s)",
				resourceManagerAddress, resourceManagerLeaderId); 
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

		private final String taskExecutorAddress;
		
		private final ResourceID resourceID;

		public ResourceManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,
				UUID leaderId,
				String taskExecutorAddress,
				ResourceID resourceID) {

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, leaderId);
			this.taskExecutorAddress = checkNotNull(taskExecutorAddress);
			this.resourceID = checkNotNull(resourceID);
		}

		@Override
		protected Future<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway resourceManager, UUID leaderId, long timeoutMillis) throws Exception {

			FiniteDuration timeout = new FiniteDuration(timeoutMillis, TimeUnit.MILLISECONDS);
			return resourceManager.registerTaskExecutor(leaderId, taskExecutorAddress, resourceID, timeout);
		}
	}
}
