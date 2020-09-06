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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Information provided by the TaskExecutor when it registers to the ResourceManager.
 */
public class TaskExecutorRegistration implements Serializable {
	private static final long serialVersionUID = -5727832919954047964L;

	/**
	 * The address of the TaskExecutor that registers.
	 */
	private final String taskExecutorAddress;

	/**
	 * The resource ID of the TaskExecutor that registers.
	 */
	private final ResourceID resourceId;

	/**
	 * Port used for data communication between TaskExecutors.
	 */
	private final int dataPort;

	/**
	 * Port used for JMX RMI.
	 */
	private final int jmxPort;

	/**
	 * HardwareDescription of the registering TaskExecutor.
	 */
	private final HardwareDescription hardwareDescription;

	/**
	 * Memory configuration of the registering TaskExecutor.
	 */
	private final TaskExecutorMemoryConfiguration memoryConfiguration;

	/**
	 * The default resource profile for slots requested with unknown resource requirements.
	 */
	private final ResourceProfile defaultSlotResourceProfile;

	/**
	 * The task executor total resource profile.
	 */
	private final ResourceProfile totalResourceProfile;

	public TaskExecutorRegistration(
			final String taskExecutorAddress,
			final ResourceID resourceId,
			final int dataPort,
			final int jmxPort,
			final HardwareDescription hardwareDescription,
			final TaskExecutorMemoryConfiguration memoryConfiguration,
			final ResourceProfile defaultSlotResourceProfile,
			final ResourceProfile totalResourceProfile) {
		this.taskExecutorAddress = checkNotNull(taskExecutorAddress);
		this.resourceId = checkNotNull(resourceId);
		this.dataPort = dataPort;
		this.jmxPort = jmxPort;
		this.hardwareDescription = checkNotNull(hardwareDescription);
		this.memoryConfiguration = checkNotNull(memoryConfiguration);
		this.defaultSlotResourceProfile = checkNotNull(defaultSlotResourceProfile);
		this.totalResourceProfile = checkNotNull(totalResourceProfile);
	}

	public String getTaskExecutorAddress() {
		return taskExecutorAddress;
	}

	public ResourceID getResourceId() {
		return resourceId;
	}

	public int getDataPort() {
		return dataPort;
	}

	public int getJmxPort() {
		return jmxPort;
	}

	public HardwareDescription getHardwareDescription() {
		return hardwareDescription;
	}

	public TaskExecutorMemoryConfiguration getMemoryConfiguration() {
		return memoryConfiguration;
	}

	public ResourceProfile getDefaultSlotResourceProfile() {
		return defaultSlotResourceProfile;
	}

	public ResourceProfile getTotalResourceProfile() {
		return totalResourceProfile;
	}
}
