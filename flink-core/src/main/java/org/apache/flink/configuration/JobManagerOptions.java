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
	 * This option specifies the interval in order to trigger a resource manager reconnection if the connection
	 * to the resource manager has been lost.
	 *
	 * This option is only intended for internal use.
	 */
	public static final ConfigOption<Long> RESOURCE_MANAGER_RECONNECT_INTERVAL =
		key("jobmanager.resourcemanager.reconnect-interval")
		.defaultValue(2000L);

	// ------------------------------------------------------------------------
	//  JobManager web UI
	// ------------------------------------------------------------------------

	/**
	 * The port for the runtime monitor web-frontend server.
	 */
	public static final ConfigOption<Integer> WEB_PORT =
		key("jobmanager.web.port")
		.defaultValue(8081);

	/**
	 * Config parameter to override SSL support for the JobManager Web UI
	 */
	public static final ConfigOption<Boolean> WEB_SSL_ENABLED =
		key("jobmanager.web.ssl.enabled")
		.defaultValue(true);

	/**
	 * The config parameter defining the flink web directory to be used by the webmonitor.
	 */
	public static final ConfigOption<String> WEB_TMP_DIR =
		key("jobmanager.web.tmpdir")
		.defaultValue(System.getProperty("java.io.tmpdir"));

	/**
	 * The config parameter defining the directory for uploading the job jars. If not specified a dynamic directory
	 * will be used under the directory specified by JOB_MANAGER_WEB_TMPDIR_KEY.
	 */
	public static final ConfigOption<String> WEB_UPLOAD_DIR =
		key("jobmanager.web.upload.dir")
		.noDefaultValue();

	/**
	 * The config parameter defining the number of archived jobs for the jobmanager.
	 */
	public static final ConfigOption<Integer> WEB_ARCHIVE_COUNT =
		key("jobmanager.web.history")
		.defaultValue(5);

	/**
	 * The log file location (may be in /log for standalone but under log directory when using YARN).
	 */
	public static final ConfigOption<String> WEB_LOG_PATH =
		key("jobmanager.web.log.path")
		.noDefaultValue();

	/**
	 * Config parameter indicating whether jobs can be uploaded and run from the web-frontend.
	 */
	public static final ConfigOption<Boolean> WEB_SUBMIT_ENABLE =
		key("jobmanager.web.submit.enable")
		.defaultValue(true);

	/**
	 * Config parameter defining the number of checkpoints to remember for recent history.
	 */
	public static final ConfigOption<Integer> WEB_CHECKPOINTS_HISTORY_SIZE =
		key("jobmanager.web.checkpoints.history")
		.defaultValue(10);

	/**
	 * Time after which cached stats are cleaned up if not accessed.
	 */
	public static final ConfigOption<Integer> WEB_BACKPRESSURE_CLEANUP_INTERVAL =
		key("jobmanager.web.backpressure.cleanup-interval")
		.defaultValue(10 * 60 * 1000);

	/**
	 * Time after which available stats are deprecated and need to be refreshed (by resampling).
	 */
	public static final ConfigOption<Integer> WEB_BACKPRESSURE_REFRESH_INTERVAL =
		key("jobmanager.web.backpressure.refresh-interval")
		.defaultValue(60 * 1000);

	/**
	 * Number of stack trace samples to take to determine back pressure.
	 */
	public static final ConfigOption<Integer> WEB_BACKPRESSURE_NUM_SAMPLES =
		key("jobmanager.web.backpressure.num-samples")
		.defaultValue(100);

	/**
	 * Delay between stack trace samples to determine back pressure.
	 */
	public static final ConfigOption<Integer> WEB_BACKPRESSURE_DELAY =
		key("jobmanager.web.backpressure.delay-between-samples")
		.defaultValue(50);

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
