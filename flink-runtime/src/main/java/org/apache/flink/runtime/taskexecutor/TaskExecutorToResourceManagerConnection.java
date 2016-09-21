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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.AcceptFunction;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import org.slf4j.Logger;

import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The connection between a TaskExecutor and the ResourceManager.
 */
public class TaskExecutorToResourceManagerConnection {

	/** the logger for all log messages of this class */
	private final Logger log;

	/** the TaskExecutor whose connection to the ResourceManager this represents */
	private final TaskExecutor taskExecutor;

	private final UUID resourceManagerLeaderId;

	private final String resourceManagerAddress;

	/** Execution context to be used to execute the on complete action of the ResourceManagerRegistration */
	private final Executor executor;

	private TaskExecutorToResourceManagerConnection.ResourceManagerRegistration pendingRegistration;

	private volatile ResourceManagerGateway registeredResourceManager;

	private InstanceID registrationId;

	/** flag indicating that the connection is closed */
	private volatile boolean closed;


	public TaskExecutorToResourceManagerConnection(
		Logger log,
		TaskExecutor taskExecutor,
		String resourceManagerAddress,
		UUID resourceManagerLeaderId,
		Executor executor) {

		this.log = checkNotNull(log);
		this.taskExecutor = checkNotNull(taskExecutor);
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress);
		this.resourceManagerLeaderId = checkNotNull(resourceManagerLeaderId);
		this.executor = checkNotNull(executor);
	}

	// ------------------------------------------------------------------------
	//  Life cycle
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public void start() {
		checkState(!closed, "The connection is already closed");
		checkState(!isRegistered() && pendingRegistration == null, "The connection is already started");

		pendingRegistration = new TaskExecutorToResourceManagerConnection.ResourceManagerRegistration(
				log, taskExecutor.getRpcService(),
				resourceManagerAddress, resourceManagerLeaderId,
				taskExecutor.getAddress(), taskExecutor.getResourceID());
		pendingRegistration.startRegistration();

		Future<Tuple2<ResourceManagerGateway, TaskExecutorRegistrationSuccess>> future = pendingRegistration.getFuture();

		future.thenAcceptAsync(new AcceptFunction<Tuple2<ResourceManagerGateway, TaskExecutorRegistrationSuccess>>() {
			@Override
			public void accept(Tuple2<ResourceManagerGateway, TaskExecutorRegistrationSuccess> result) {
				registrationId = result.f1.getRegistrationId();
				registeredResourceManager = result.f0;
			}
		}, executor);
		
		// this future should only ever fail if there is a bug, not if the registration is declined
		future.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
			@Override
			public Void apply(Throwable failure) {
				taskExecutor.onFatalErrorAsync(failure);
				return null;
			}
		}, executor);
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

	private static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerGateway, TaskExecutorRegistrationSuccess> {

		private final String taskExecutorAddress;
		
		private final ResourceID resourceID;

		ResourceManagerRegistration(
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

			Time timeout = Time.milliseconds(timeoutMillis);
			return resourceManager.registerTaskExecutor(leaderId, taskExecutorAddress, resourceID, timeout);
		}
	}
}
