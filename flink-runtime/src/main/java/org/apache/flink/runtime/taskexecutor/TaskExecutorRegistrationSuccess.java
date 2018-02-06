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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Base class for responses from the ResourceManager to a registration attempt by a
 * TaskExecutor.
 */
public final class TaskExecutorRegistrationSuccess extends RegistrationResponse.Success implements Serializable {

	private static final long serialVersionUID = 1L;

	private final InstanceID registrationId;

	private final ResourceID resourceManagerResourceId;

	private final long heartbeatInterval;

	private final ClusterInformation clusterInformation;

	/**
	 * Create a new {@code TaskExecutorRegistrationSuccess} message.
	 * 
	 * @param registrationId The ID that the ResourceManager assigned the registration.
	 * @param resourceManagerResourceId The unique ID that identifies the ResourceManager.
	 * @param heartbeatInterval The interval in which the ResourceManager will heartbeat the TaskExecutor.
	 * @param clusterInformation information about the cluster
	 */
	public TaskExecutorRegistrationSuccess(
			InstanceID registrationId,
			ResourceID resourceManagerResourceId,
			long heartbeatInterval,
			ClusterInformation clusterInformation) {
		this.registrationId = Preconditions.checkNotNull(registrationId);
		this.resourceManagerResourceId = Preconditions.checkNotNull(resourceManagerResourceId);
		this.heartbeatInterval = heartbeatInterval;
		this.clusterInformation = Preconditions.checkNotNull(clusterInformation);
	}

	/**
	 * Gets the ID that the ResourceManager assigned the registration.
	 */
	public InstanceID getRegistrationId() {
		return registrationId;
	}

	/**
	 * Gets the unique ID that identifies the ResourceManager.
	 */
	public ResourceID getResourceManagerId() {
		return resourceManagerResourceId;
	}

	/**
	 * Gets the interval in which the ResourceManager will heartbeat the TaskExecutor.
	 */
	public long getHeartbeatInterval() {
		return heartbeatInterval;
	}

	/**
	 * Gets the cluster information.
	 */
	public ClusterInformation getClusterInformation() {
		return clusterInformation;
	}

	@Override
	public String toString() {
		return "TaskExecutorRegistrationSuccess (" + registrationId + " / " + resourceManagerResourceId + " / " + heartbeatInterval + ')';
	}

}







