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

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.slf4j.Logger;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class TaskExecutorToResourceManagerConnection {

	private final Logger log;

	private final TaskExecutor taskExecutor;

	private final UUID resourceManagerLeaderId;

	private final String resourceManagerAddress;

	private ResourceManagerRegistration pendingRegistration;

	private ResourceManagerGateway registeredResourceManager;

	private InstanceID registrationId;

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

	public void start() {
		checkState(!closed, "The connection is already closed");
		checkState(!isRegistered() && pendingRegistration == null, "The connection is already started");

		pendingRegistration = new ResourceManagerRegistration(log, this, taskExecutor,
				resourceManagerAddress, resourceManagerLeaderId);
		pendingRegistration.resolveResourceManagerAndStartRegistration();
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

	public ResourceManagerGateway getResourceManager() {
		return registeredResourceManager;
	}

	public boolean isRegistered() {
		return registeredResourceManager != null;
	}

	// ------------------------------------------------------------------------
	//  Connection Methods
	// ------------------------------------------------------------------------

	void completedRegistrationAtResourceManager(
			ResourceManagerGateway resourceManagerGateway,
			InstanceID registrationId) {

		if (!isClosed()) {
			this.pendingRegistration = null;
			this.registeredResourceManager = resourceManagerGateway;
			this.registrationId = registrationId; 
		}
		else {
			log.debug("Completed registration at ResourceManager, but leader changed in the meantime. Ignoring.");
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Connection to ResourceManager %s (leaderId=%s)",
				resourceManagerAddress, resourceManagerLeaderId); 
	}
}
