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
import org.apache.flink.annotation.docs.Documentation;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for the WebMonitorEndpoint.
 */
@PublicEvolving
public class WebOptions {

	/**
	 * Config parameter defining the runtime monitor web-frontend server address.
	 */
	@Deprecated
	public static final ConfigOption<String> ADDRESS =
		key("web.address")
			.noDefaultValue()
			.withDeprecatedKeys("jobmanager.web.address")
			.withDescription("Address for runtime monitor web-frontend server.");

	/**
	 * The port for the runtime monitor web-frontend server.
	 *
	 * @deprecated Use {@link RestOptions#PORT} instead
	 */
	@Deprecated
	public static final ConfigOption<Integer> PORT =
		key("web.port")
			.defaultValue(8081)
			.withDeprecatedKeys("jobmanager.web.port");

	/**
	 * The config parameter defining the Access-Control-Allow-Origin header for all
	 * responses from the web-frontend.
	 */
	public static final ConfigOption<String> ACCESS_CONTROL_ALLOW_ORIGIN =
		key("web.access-control-allow-origin")
			.defaultValue("*")
			.withDeprecatedKeys("jobmanager.web.access-control-allow-origin")
			.withDescription("Access-Control-Allow-Origin header for all responses from the web-frontend.");

	/**
	 * The config parameter defining the refresh interval for the web-frontend in milliseconds.
	 */
	public static final ConfigOption<Long> REFRESH_INTERVAL =
		key("web.refresh-interval")
			.defaultValue(3000L)
			.withDeprecatedKeys("jobmanager.web.refresh-interval")
			.withDescription("Refresh interval for the web-frontend in milliseconds.");

	/**
	 * Config parameter to override SSL support for the JobManager Web UI.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> SSL_ENABLED =
		key("web.ssl.enabled")
			.defaultValue(true)
			.withDeprecatedKeys("jobmanager.web.ssl.enabled")
			.withDescription("Flag indicating whether to override SSL support for the JobManager Web UI.");

	/**
	 * The config parameter defining the flink web directory to be used by the webmonitor.
	 */
	@Documentation.OverrideDefault("System.getProperty(\"java.io.tmpdir\")")
	public static final ConfigOption<String> TMP_DIR =
		key("web.tmpdir")
			.defaultValue(System.getProperty("java.io.tmpdir"))
			.withDeprecatedKeys("jobmanager.web.tmpdir")
			.withDescription("Flink web directory which is used by the webmonitor.");

	/**
	 * The config parameter defining the directory for uploading the job jars. If not specified a dynamic directory
	 * will be used under the directory specified by JOB_MANAGER_WEB_TMPDIR_KEY.
	 */
	public static final ConfigOption<String> UPLOAD_DIR =
		key("web.upload.dir")
			.noDefaultValue()
			.withDeprecatedKeys("jobmanager.web.upload.dir")
			.withDescription("Directory for uploading the job jars. If not specified a dynamic directory will be used" +
				" under the directory specified by JOB_MANAGER_WEB_TMPDIR_KEY.");

	/**
	 * The config parameter defining the number of archived jobs for the JobManager.
	 */
	public static final ConfigOption<Integer> ARCHIVE_COUNT =
		key("web.history")
			.defaultValue(5)
			.withDeprecatedKeys("jobmanager.web.history")
			.withDescription("Number of archived jobs for the JobManager.");

	/**
	 * The log file location (may be in /log for standalone but under log directory when using YARN).
	 */
	public static final ConfigOption<String> LOG_PATH =
		key("web.log.path")
			.noDefaultValue()
			.withDeprecatedKeys("jobmanager.web.log.path")
			.withDescription("Path to the log file (may be in /log for standalone but under log directory when using YARN).");

	/**
	 * Config parameter indicating whether jobs can be uploaded and run from the web-frontend.
	 */
	public static final ConfigOption<Boolean> SUBMIT_ENABLE =
		key("web.submit.enable")
			.defaultValue(true)
			.withDeprecatedKeys("jobmanager.web.submit.enable")
			.withDescription("Flag indicating whether jobs can be uploaded and run from the web-frontend.");

	/**
	 * Config parameter defining the number of checkpoints to remember for recent history.
	 */
	public static final ConfigOption<Integer> CHECKPOINTS_HISTORY_SIZE =
		key("web.checkpoints.history")
			.defaultValue(10)
			.withDeprecatedKeys("jobmanager.web.checkpoints.history")
			.withDescription("Number of checkpoints to remember for recent history.");

	/**
	 * Time, in milliseconds, after which cached stats are cleaned up if not accessed.
	 */
	public static final ConfigOption<Integer> BACKPRESSURE_CLEANUP_INTERVAL =
		key("web.backpressure.cleanup-interval")
			.defaultValue(10 * 60 * 1000)
			.withDeprecatedKeys("jobmanager.web.backpressure.cleanup-interval")
			.withDescription("Time, in milliseconds, after which cached stats are cleaned up if not accessed.");

	/**
	 * Time, in milliseconds, after which available stats are deprecated and need to be refreshed (by resampling).
	 */
	public static final ConfigOption<Integer> BACKPRESSURE_REFRESH_INTERVAL =
		key("web.backpressure.refresh-interval")
			.defaultValue(60 * 1000)
			.withDeprecatedKeys("jobmanager.web.backpressure.refresh-interval")
			.withDescription("Time, in milliseconds, after which available stats are deprecated and need to be refreshed" +
				" (by resampling).");

	/**
	 * Number of samples to take to determine back pressure.
	 */
	public static final ConfigOption<Integer> BACKPRESSURE_NUM_SAMPLES =
		key("web.backpressure.num-samples")
			.defaultValue(100)
			.withDeprecatedKeys("jobmanager.web.backpressure.num-samples")
			.withDescription("Number of samples to take to determine back pressure.");

	/**
	 * Delay between samples to determine back pressure in milliseconds.
	 */
	public static final ConfigOption<Integer> BACKPRESSURE_DELAY =
		key("web.backpressure.delay-between-samples")
			.defaultValue(50)
			.withDeprecatedKeys("jobmanager.web.backpressure.delay-between-samples")
			.withDescription("Delay between samples to determine back pressure in milliseconds.");

	/**
	 * Timeout for asynchronous operations by the web monitor in milliseconds.
	 */
	public static final ConfigOption<Long> TIMEOUT =
		key("web.timeout")
		.defaultValue(10L * 60L * 1000L)
		.withDescription("Timeout for asynchronous operations by the web monitor in milliseconds.");

	// ------------------------------------------------------------------------

	/** Not meant to be instantiated. */
	private WebOptions() {}
}
