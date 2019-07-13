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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;

/**
 * Utils for ResourceManager.
 */
public class ResourceManagerUtil {

	public static Configuration getResourceManagerConfiguration(Configuration flinkConfig) {
		final int taskManagerMemoryMB = ConfigurationUtils.getTaskManagerHeapMemory(flinkConfig).getMebiBytes();
		final long cutoffMB = ContaineredTaskManagerParameters.calculateCutoffMB(flinkConfig, taskManagerMemoryMB);
		final long processMemoryBytes = (taskManagerMemoryMB - cutoffMB) << 20; // megabytes to bytes
		final long managedMemoryBytes = TaskManagerServices.getManagedMemoryFromProcessMemory(flinkConfig, processMemoryBytes);

		final Configuration resourceManagerConfig = new Configuration(flinkConfig);
		resourceManagerConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, managedMemoryBytes + "b");

		return resourceManagerConfig;
	}

	private ResourceManagerUtil() {
	}
}
