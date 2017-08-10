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

import static org.apache.flink.configuration.ConfigOptions.key;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Configuration options for the JobManager.
 */
@PublicEvolving
public class JobManagerOptions {

	/**
	 * The config parameter defining the network address to connect to
	 * for communication with the job manager.
	 * 
	 * <p>This value is only interpreted in setups where a single JobManager with static 
	 * name or address exists (simple standalone setups, or container setups with dynamic
	 * service name resolution). It is not used in many high-availability setups, when a
	 * leader-election service (like ZooKeeper) is used to elect and discover the JobManager
	 * leader from potentially multiple standby JobManagers.
	 */
	public static final ConfigOption<String> ADDRESS =
		key("jobmanager.rpc.address")
		.noDefaultValue();

	/**
	 * The config parameter defining the network port to connect to
	 * for communication with the job manager.
	 * 
	 * <p>Like {@link JobManagerOptions#ADDRESS}, this value is only interpreted in setups where
	 * a single JobManager with static name/address and port exists (simple standalone setups,
	 * or container setups with dynamic service name resolution).
	 * This config option is not used in many high-availability setups, when a
	 * leader-election service (like ZooKeeper) is used to elect and discover the JobManager
	 * leader from potentially multiple standby JobManagers.
	 */
	public static final ConfigOption<Integer> PORT =
		key("jobmanager.rpc.port")
		.defaultValue(6123);

	/**
	 * JVM heap size (in megabytes) for the JobManager
	 */
	public static final ConfigOption<Integer> JOB_MANAGER_HEAP_MEMORY =
		key("jobmanager.heap.mb")
		.defaultValue(1024);

	/**
	 * The maximum number of prior execution attempts kept in history.
	 */
	public static final ConfigOption<Integer> MAX_ATTEMPTS_HISTORY_SIZE =
		key("jobmanager.execution.attempts-history-size")
			.defaultValue(16)
			.withDeprecatedKeys("job-manager.max-attempts-history-size");

	/**
	 * The maximum number of prior execution attempts kept in history.
	 */
	public static final ConfigOption<String> EXECUTION_FAILOVER_STRATEGY =
		key("jobmanager.execution.failover-strategy")
			.defaultValue("full");

	/**
	 * This option specifies the interval in order to trigger a resource manager reconnection if the connection
	 * to the resource manager has been lost.
	 *
	 * This option is only intended for internal use.
	 */
	public static final ConfigOption<Long> RESOURCE_MANAGER_RECONNECT_INTERVAL =
		key("jobmanager.resourcemanager.reconnect-interval")
		.defaultValue(2000L);

	/**
	 * The location where the JobManager stores the archives of completed jobs.
	 */
	public static final ConfigOption<String> ARCHIVE_DIR =
		key("jobmanager.archive.fs.dir")
			.noDefaultValue();

	// ---------------------------------------------------------------------------------------------

	private JobManagerOptions() {
		throw new IllegalAccessError();
	}
}
