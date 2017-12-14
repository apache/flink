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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The set of configuration options relating to the ResourceManager
 */
@PublicEvolving
public class ResourceManagerOptions {

	/**
	 * Timeout for jobs which don't have a job manager as leader assigned.
	 */
	public static final ConfigOption<String> JOB_TIMEOUT = ConfigOptions
		.key("resourcemanager.job.timeout")
		.defaultValue("5 minutes");

	public static final ConfigOption<Integer> LOCAL_NUMBER_RESOURCE_MANAGER = ConfigOptions
		.key("local.number-resourcemanager")
		.defaultValue(1);

	public static final ConfigOption<Integer> IPC_PORT = ConfigOptions
		.key("resourcemanager.rpc.port")
		.defaultValue(0);

	/**
	 * Percentage of heap space to remove from containers (YARN / Mesos), to compensate
	 * for other JVM memory usage.
	 */
	public static final ConfigOption<Float> CONTAINERIZED_HEAP_CUTOFF_RATIO = ConfigOptions
		.key("containerized.heap-cutoff-ratio")
		.defaultValue(0.25f)
		.withDeprecatedKeys("yarn.heap-cutoff-ratio");

	/**
	 * Minimum amount of heap memory to remove in containers, as a safety margin.
	 */
	public static final ConfigOption<Integer> CONTAINERIZED_HEAP_CUTOFF_MIN = ConfigOptions
		.key("containerized.heap-cutoff-min")
		.defaultValue(600)
		.withDeprecatedKeys("yarn.heap-cutoff-min");

	/**
	 * Prefix for passing custom environment variables to Flink's master process.
	 * For example for passing LD_LIBRARY_PATH as an env variable to the AppMaster, set:
	 * containerized.master.env.LD_LIBRARY_PATH: "/usr/lib/native"
	 * in the flink-conf.yaml.
	 */
	public static final String CONTAINERIZED_MASTER_ENV_PREFIX = "containerized.master.env.";

	/**
	 * Similar to the {@see CONTAINERIZED_MASTER_ENV_PREFIX}, this configuration prefix allows
	 * setting custom environment variables for the workers (TaskManagers)
	 */
	public static final String CONTAINERIZED_TASK_MANAGER_ENV_PREFIX = "containerized.taskmanager.env.";
	
	// ---------------------------------------------------------------------------------------------

	/** Not intended to be instantiated */
	private ResourceManagerOptions() {}
}
