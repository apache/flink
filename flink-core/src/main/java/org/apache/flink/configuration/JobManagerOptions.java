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
 * Configuration options for the JobManager.
 */
@PublicEvolving
public class JobManagerOptions {

	/**
	 * The config parameter defining the network address to connect to
	 * for communication with the job manager.
	 */
	public static final ConfigOption<String> ADDRESS = ConfigOptions
		.key("jobmanager.rpc.address")
		.noDefaultValue();

	/**
	 * The config parameter defining the network port to connect to
	 * for communication with the job manager.
	 */
	public static final ConfigOption<Integer> PORT = ConfigOptions
		.key("jobmanager.rpc.port")
		.defaultValue(6123);

	/**
	 * The port for the runtime monitor web-frontend server.
	 */
	public static final ConfigOption<Integer> WEB_PORT = ConfigOptions
		.key("jobmanager.web.port")
		.defaultValue(8081);

	/**
	 * Config parameter to override SSL support for the JobManager Web UI
	 */
	public static final ConfigOption<Boolean> WEB_SSL_ENABLED = ConfigOptions
		.key("jobmanager.web.ssl.enabled")
		.defaultValue(true);

	/**
	 * The config parameter defining the flink web directory to be used by the webmonitor.
	 */
	public static final ConfigOption<String> WEB_TMP_DIR = ConfigOptions
		.key("jobmanager.web.tmpdir")
		.defaultValue(System.getProperty("java.io.tmpdir"));

	/**
	 * The config parameter defining the directory for uploading the job jars. If not specified a dynamic directory
	 * will be used under the directory specified by JOB_MANAGER_WEB_TMPDIR_KEY.
	 */
	public static final ConfigOption<String> WEB_UPLOAD_DIR = ConfigOptions
		.key("jobmanager.web.upload.dir")
		.noDefaultValue();

	/**
	 * The config parameter defining the number of archived jobs for the jobmanager.
	 */
	public static final ConfigOption<Integer> WEB_ARCHIVE_COUNT = ConfigOptions
		.key("jobmanager.web.history")
		.defaultValue(5);

	/**
	 * The log file location (may be in /log for standalone but under log directory when using YARN).
	 */
	public static final ConfigOption<String> WEB_LOG_PATH = ConfigOptions
		.key("jobmanager.web.log.path")
		.noDefaultValue();

	/**
	 * Config parameter indicating whether jobs can be uploaded and run from the web-frontend.
	 */
	public static final ConfigOption<Boolean> WEB_SUBMIT_ENABLE = ConfigOptions
		.key("jobmanager.web.submit.enable")
		.defaultValue(true);

	/**
	 * Config parameter defining the number of checkpoints to remember for recent history.
	 */
	public static final ConfigOption<Integer> WEB_CHECKPOINTS_HISTORY_SIZE = ConfigOptions
		.key("jobmanager.web.checkpoints.history")
		.defaultValue(10);

	/**
	 * Time after which cached stats are cleaned up if not accessed.
	 */
	public static final ConfigOption<Integer> WEB_BACKPRESSURE_CLEANUP_INTERVAL = ConfigOptions
		.key("jobmanager.web.backpressure.cleanup-interval")
		.defaultValue(10 * 60 * 1000);

	/**
	 * Time after which available stats are deprecated and need to be refreshed (by resampling).
	 */
	public static final ConfigOption<Integer> WEB_BACKPRESSURE_REFRESH_INTERVAL = ConfigOptions
		.key("jobmanager.web.backpressure.refresh-interval")
		.defaultValue(60 * 1000);

	/**
	 * Number of stack trace samples to take to determine back pressure.
	 */
	public static final ConfigOption<Integer> WEB_BACKPRESSURE_NUM_SAMPLES = ConfigOptions
		.key("jobmanager.web.backpressure.num-samples")
		.defaultValue(100);

	/**
	 * Delay between stack trace samples to determine back pressure.
	 */
	public static final ConfigOption<Integer> WEB_BACKPRESSURE_DELAY = ConfigOptions
		.key("jobmanager.web.backpressure.delay-between-samples")
		.defaultValue(50);

	/**
	 * The maximum number of prior execution attempts kept in history.
	 */
	public static final ConfigOption<Integer> MAX_ATTEMPTS_HISTORY_SIZE = ConfigOptions
		.key("job-manager.max-attempts-history-size")
		.defaultValue(16);

	// ---------------------------------------------------------------------------------------------

	private JobManagerOptions() {
		throw new IllegalAccessError();
	}
}
