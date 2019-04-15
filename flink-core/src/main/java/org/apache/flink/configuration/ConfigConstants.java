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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class contains all constants for the configuration. That includes the configuration keys and
 * the default values.
 */
@Public
@SuppressWarnings("unused")
public final class ConfigConstants {

	// ------------------------------------------------------------------------
	//                            Configuration Keys
	// ------------------------------------------------------------------------

	// ---------------------------- Restart strategies ------------------------

	/**
	 * Defines the restart strategy to be used. It can be "off", "none", "disable" to be disabled or
	 * it can be "fixeddelay", "fixed-delay" to use the FixedDelayRestartStrategy or it can
	 * be "failurerate", "failure-rate" to use FailureRateRestartStrategy. You can also
	 * specify a class name which implements the RestartStrategy interface and has a static
	 * create method which takes a Configuration object.
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY = "restart-strategy";

	/**
	 * Maximum number of attempts the fixed delay restart strategy will try before failing a job.
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS = "restart-strategy.fixed-delay.attempts";

	/**
	 * Delay between two consecutive restart attempts in FixedDelayRestartStrategy. It can be specified using Scala's
	 * FiniteDuration notation: "1 min", "20 s"
	 */
	@PublicEvolving
	public static final ConfigOption<String> RESTART_STRATEGY_FIXED_DELAY_DELAY =
		key("restart-strategy.fixed-delay.delay").defaultValue("0 s");

	/**
	 * Maximum number of restarts in given time interval {@link #RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL} before failing a job
	 * in FailureRateRestartStrategy.
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL = "restart-strategy.failure-rate.max-failures-per-interval";

	/**
	 * Time interval in which greater amount of failures than {@link #RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL} causes
	 * job fail in FailureRateRestartStrategy. It can be specified using Scala's FiniteDuration notation: "1 min", "20 s"
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL = "restart-strategy.failure-rate.failure-rate-interval";

	/**
	 * Delay between two consecutive restart attempts in FailureRateRestartStrategy.
	 * It can be specified using Scala's FiniteDuration notation: "1 min", "20 s".
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY_FAILURE_RATE_DELAY = "restart-strategy.failure-rate.delay";

	// -------------------------------- Runtime -------------------------------

	/**
	 * The config parameter defining the taskmanager log file location.
	 */
	public static final String TASK_MANAGER_LOG_PATH_KEY = "taskmanager.log.path";

	/**
	 * The config parameter defining the timeout for filesystem stream opening.
	 * A value of 0 indicates infinite waiting.
	 */
	public static final String FS_STREAM_OPENING_TIMEOUT_KEY = "taskmanager.runtime.fs_timeout";

	/**
	 * Whether to use the LargeRecordHandler when spilling.
	 */
	public static final String USE_LARGE_RECORD_HANDLER_KEY = "taskmanager.runtime.large-record-handler";


	// -------- Common Resource Framework Configuration (YARN & Mesos) --------

	/**
	 * Percentage of heap space to remove from containers (YARN / Mesos), to compensate
	 * for other JVM memory usage.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_HEAP_CUTOFF_RATIO} instead.
	 */
	@Deprecated
	public static final String CONTAINERIZED_HEAP_CUTOFF_RATIO = "containerized.heap-cutoff-ratio";

	/**
	 * Minimum amount of heap memory to remove in containers, as a safety margin.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_HEAP_CUTOFF_MIN} instead.
	 */
	@Deprecated
	public static final String CONTAINERIZED_HEAP_CUTOFF_MIN = "containerized.heap-cutoff-min";

	// ------------------------ YARN Configuration ------------------------

	/**
	 * Percentage of heap space to remove from containers started by YARN.
	 * @deprecated in favor of {@code #CONTAINERIZED_HEAP_CUTOFF_RATIO}
	 */
	@Deprecated
	public static final String YARN_HEAP_CUTOFF_RATIO = "yarn.heap-cutoff-ratio";

	/**
	 * Minimum amount of memory to remove from the heap space as a safety margin.
	 * @deprecated in favor of {@code #CONTAINERIZED_HEAP_CUTOFF_MIN}
	 */
	@Deprecated
	public static final String YARN_HEAP_CUTOFF_MIN = "yarn.heap-cutoff-min";

	/**
	 * Prefix for passing custom environment variables to Flink's ApplicationMaster (JobManager).
	 * For example for passing LD_LIBRARY_PATH as an env variable to the AppMaster, set:
	 * 	yarn.application-master.env.LD_LIBRARY_PATH: "/usr/lib/native"
	 * in the flink-conf.yaml.
	 * @deprecated Please use {@code CONTAINERIZED_MASTER_ENV_PREFIX}.
	 */
	@Deprecated
	public static final String YARN_APPLICATION_MASTER_ENV_PREFIX = "yarn.application-master.env.";

	/**
	 * Similar to the {@see YARN_APPLICATION_MASTER_ENV_PREFIX}, this configuration prefix allows
	 * setting custom environment variables.
	 * @deprecated Please use {@code CONTAINERIZED_TASK_MANAGER_ENV_PREFIX}.
	 */
	@Deprecated
	public static final String YARN_TASK_MANAGER_ENV_PREFIX = "yarn.taskmanager.env.";

	/**
	 * Template for the YARN container start invocation.
	 */
	public static final String YARN_CONTAINER_START_COMMAND_TEMPLATE =
		"yarn.container-start-command-template";

	// ------------------------ Hadoop Configuration ------------------------

	/**
	 * Path to hdfs-default.xml file.
	 *
	 * @deprecated Use environment variable HADOOP_CONF_DIR instead.
	 */
	@Deprecated
	public static final String HDFS_DEFAULT_CONFIG = "fs.hdfs.hdfsdefault";

	/**
	 * Path to hdfs-site.xml file.
	 *
	 * @deprecated Use environment variable HADOOP_CONF_DIR instead.
	 */
	@Deprecated
	public static final String HDFS_SITE_CONFIG = "fs.hdfs.hdfssite";

	/**
	 * Path to Hadoop configuration.
	 *
	 * @deprecated Use environment variable HADOOP_CONF_DIR instead.
	 */
	@Deprecated
	public static final String PATH_HADOOP_CONFIG = "fs.hdfs.hadoopconf";

	// ------------------------- JobManager Web Frontend ----------------------

	/**
	 * The port for the runtime monitor web-frontend server.
	 *
	 * @deprecated Use {@link WebOptions#PORT} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_PORT_KEY = "jobmanager.web.port";

	// ---------------------------- Metrics -----------------------------------

	/**
	 * The prefix for per-reporter configs. Has to be combined with a reporter name and
	 * the configs mentioned below.
	 */
	public static final String METRICS_REPORTER_PREFIX = "metrics.reporter.";

	/** The class of the reporter to use. This is used as a suffix in an actual reporter config */
	public static final String METRICS_REPORTER_CLASS_SUFFIX = "class";

	/** The interval between reports. This is used as a suffix in an actual reporter config */
	public static final String METRICS_REPORTER_INTERVAL_SUFFIX = "interval";

	/**	The delimiter used to assemble the metric identifier. This is used as a suffix in an actual reporter config. */
	public static final String METRICS_REPORTER_SCOPE_DELIMITER = "scope.delimiter";

	// ------------------------------------------------------------------------
	//                            Default Values
	// ------------------------------------------------------------------------

	// ---------------------------- Parallelism -------------------------------

	/**
	 * The default number of execution retries.
	 */
	public static final int DEFAULT_EXECUTION_RETRIES = 0;

	// ------------------------ Runtime Algorithms ------------------------

	/**
	 * The default timeout for filesystem stream opening: infinite (means max long milliseconds).
	 */
	public static final int DEFAULT_FS_STREAM_OPENING_TIMEOUT = 0;

	/**
	 * Whether to use the LargeRecordHandler when spilling.
	 */
	public static final boolean DEFAULT_USE_LARGE_RECORD_HANDLER = false;


	// ------ Common Resource Framework Configuration (YARN & Mesos) ------

	/**
	 * Minimum amount of memory to subtract from the process memory to get the TaskManager
	 * heap size. We came up with these values experimentally.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_HEAP_CUTOFF_MIN} instead.
	 */
	@Deprecated
	public static final int DEFAULT_YARN_HEAP_CUTOFF = 600;

	/**
	 * Relative amount of memory to subtract from Java process memory to get the TaskManager
	 * heap size.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_HEAP_CUTOFF_RATIO} instead.
	 */
	@Deprecated
	public static final float DEFAULT_YARN_HEAP_CUTOFF_RATIO = 0.25f;

	/**
	 * Start command template for Flink on YARN containers.
	 */
	public static final String DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE =
		"%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%";

	// ------------------------ File System Behavior ------------------------

	/**
	 * The default filesystem to be used, if no other scheme is specified in the
	 * user-provided URI (= local filesystem).
	 */
	public static final String DEFAULT_FILESYSTEM_SCHEME = "file:///";

	/**
	 * The default behavior with respect to overwriting existing files (= not overwrite).
	 */
	public static final boolean DEFAULT_FILESYSTEM_OVERWRITE = false;

	// ------------------------- JobManager Web Frontend ----------------------

	/**
	 * The config key for the address of the JobManager web frontend.
	 *
	 * @deprecated use {@link WebOptions#ADDRESS} instead
	 */
	@Deprecated
	public static final ConfigOption<String> DEFAULT_JOB_MANAGER_WEB_FRONTEND_ADDRESS =
		key("jobmanager.web.address")
			.noDefaultValue();

	// ----------------------------- Streaming Values --------------------------

	public static final String DEFAULT_STATE_BACKEND = "jobmanager";

	// ----------------------------- LocalExecution ----------------------------

	/**
	 * Sets the number of local task managers.
	 */
	public static final String LOCAL_NUMBER_TASK_MANAGER = "local.number-taskmanager";

	public static final int DEFAULT_LOCAL_NUMBER_TASK_MANAGER = 1;

	public static final String LOCAL_NUMBER_JOB_MANAGER = "local.number-jobmanager";

	public static final int DEFAULT_LOCAL_NUMBER_JOB_MANAGER = 1;

	public static final String LOCAL_START_WEBSERVER = "local.start-webserver";

	// --------------------------- High Availability ---------------------------------

	/** @deprecated Deprecated in favour of {@link HighAvailabilityOptions#HA_MODE} */
	@PublicEvolving
	@Deprecated
	public static final String DEFAULT_HA_MODE = "none";

	/** @deprecated Deprecated in favour of {@link #DEFAULT_HA_MODE} */
	@Deprecated
	public static final String DEFAULT_RECOVERY_MODE = "standalone";

	// ----------------------------- Environment Variables ----------------------------

	/** The environment variable name which contains the location of the configuration directory. */
	public static final String ENV_FLINK_CONF_DIR = "FLINK_CONF_DIR";

	/** The environment variable name which contains the location of the lib folder. */
	public static final String ENV_FLINK_LIB_DIR = "FLINK_LIB_DIR";

	/** The environment variable name which contains the location of the bin directory. */
	public static final String ENV_FLINK_BIN_DIR = "FLINK_BIN_DIR";

	/** The environment variable name which contains the Flink installation root directory. */
	public static final String ENV_FLINK_HOME_DIR = "FLINK_HOME";

	// ---------------------------- Encoding ------------------------------

	public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

	/**
	 * Not instantiable.
	 */
	private ConfigConstants() {}
}
