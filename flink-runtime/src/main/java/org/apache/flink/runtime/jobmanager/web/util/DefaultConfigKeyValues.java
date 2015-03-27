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

package org.apache.flink.runtime.jobmanager.web.util;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import scala.concurrent.duration.Duration;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

public final class DefaultConfigKeyValues {

	public static final Set<String> INT_FIELD_KEYS = new HashSet<String>(Arrays.asList(
		ConfigConstants.DEFAULT_PARALLELISM_KEY,
		ConfigConstants.DEFAULT_EXECUTION_RETRIES_KEY,
		ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
		ConfigConstants.BLOB_FETCH_RETRIES_KEY,
		ConfigConstants.BLOB_FETCH_CONCURRENT_KEY,
		ConfigConstants.BLOB_FETCH_BACKLOG_KEY,
		ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
		ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
		ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY,
		ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
		ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
		ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS,
		ConfigConstants.DEFAULT_SPILLING_MAX_FAN_KEY,
		ConfigConstants.FS_STREAM_OPENING_TIMEOUT_KEY,
		ConfigConstants.YARN_HEAP_LIMIT_CAP,
		ConfigConstants.DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY,
		ConfigConstants.DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY,
		ConfigConstants.DELIMITED_FORMAT_MAX_SAMPLE_LENGTH_KEY,
		ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
		ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT,
		ConfigConstants.WEB_FRONTEND_PORT_KEY,
		ConfigConstants.AKKA_DISPATCHER_THROUGHPUT,
		ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER,
		ConfigConstants.YARN_APPLICATION_ATTEMPTS,
		ConfigConstants.YARN_HEARTBEAT_DELAY_SECONDS,
		ConfigConstants.YARN_MAX_FAILED_CONTAINERS
	));

	public static final Set<String> LONG_FIELD_KEYS = new HashSet<String>(Arrays.asList(
		ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
		ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS
	));

	public static final Set<String> FLOAT_FIELD_KEYS = new HashSet<String>(Arrays.asList(
		ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
		ConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD_KEY,
		ConfigConstants.YARN_HEAP_CUTOFF_RATIO
	));

	public static final Set<String> DOUBLE_FIELD_KEYS = new HashSet<String>(Arrays.asList(
		ConfigConstants.AKKA_WATCH_THRESHOLD,
		ConfigConstants.AKKA_WATCH_THRESHOLD
	));

	public static final Set<String> BOOLEAN_FIELD_KEYS = new HashSet<String>(Arrays.asList(
		ConfigConstants.TASK_MANAGER_MEMORY_LAZY_ALLOCATION_KEY,
		ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD,
		ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY,
		ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY,
		ConfigConstants.AKKA_LOG_LIFECYCLE_EVENTS,
		ConfigConstants.YARN_REALLOCATE_FAILED_CONTAINERS
	));

	public static Map<String, Object> getDefaultConfig(Configuration user) {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(ConfigConstants.DEFAULT_PARALLELISM_KEY, ConfigConstants.DEFAULT_PARALLELISM);
		config.put(ConfigConstants.DEFAULT_EXECUTION_RETRIES_KEY, ConfigConstants.DEFAULT_EXECUTION_RETRIES);
		config.put(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY,
			user.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT));
		config.put(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		config.put(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
		config.put(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, null);
		config.put(ConfigConstants.BLOB_FETCH_RETRIES_KEY, ConfigConstants.DEFAULT_BLOB_FETCH_RETRIES);
		config.put(ConfigConstants.BLOB_FETCH_CONCURRENT_KEY, ConfigConstants.DEFAULT_BLOB_FETCH_CONCURRENT);
		config.put(ConfigConstants.BLOB_FETCH_BACKLOG_KEY, ConfigConstants.DEFAULT_BLOB_FETCH_BACKLOG);
		config.put(ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL, ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL);
		config.put(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null);
		config.put(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT);
		config.put(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT);
		config.put(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
		config.put(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1);
		config.put(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION);
		config.put(ConfigConstants.TASK_MANAGER_MEMORY_LAZY_ALLOCATION_KEY, true);
		config.put(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS);
		config.put(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE);
		config.put(ConfigConstants.TASK_MANAGER_NETWORK_DEFAULT_IO_MODE, ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_DEFAULT_IO_MODE);
		config.put(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
		config.put(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD, ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD);
		config.put(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS, ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS);
		config.put(ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION, ConfigConstants.DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION);
		config.put(ConfigConstants.DEFAULT_SPILLING_MAX_FAN_KEY, ConfigConstants.DEFAULT_SPILLING_MAX_FAN);
		config.put(ConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD_KEY, ConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD);
		config.put(ConfigConstants.FS_STREAM_OPENING_TIMEOUT_KEY, ConfigConstants.DEFAULT_FS_STREAM_OPENING_TIMEOUT);
		config.put(ConfigConstants.YARN_HEAP_CUTOFF_RATIO, 0.8f);
		config.put(ConfigConstants.YARN_HEAP_LIMIT_CAP, 500);
		config.put(ConfigConstants.YARN_REALLOCATE_FAILED_CONTAINERS, true);
		config.put(ConfigConstants.YARN_MAX_FAILED_CONTAINERS, System.getenv().get("_CLIENT_TM_COUNT"));
		config.put(ConfigConstants.YARN_APPLICATION_ATTEMPTS, 1);
		config.put(ConfigConstants.YARN_HEARTBEAT_DELAY_SECONDS, 5);
		config.put(ConfigConstants.HDFS_DEFAULT_CONFIG, null);
		config.put(ConfigConstants.HDFS_SITE_CONFIG, null);
		config.put(ConfigConstants.PATH_HADOOP_CONFIG, null);
		config.put(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, ConfigConstants.DEFAULT_FILESYSTEM_OVERWRITE);
		config.put(ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY, ConfigConstants.DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY);
		config.put(ConfigConstants.DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY, ConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES);
		config.put(ConfigConstants.DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY, ConfigConstants.DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES);
		config.put(ConfigConstants.DELIMITED_FORMAT_MAX_SAMPLE_LENGTH_KEY, ConfigConstants.DEFAULT_DELIMITED_FORMAT_MAX_SAMPLE_LEN);
		config.put(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT);
		config.put(ConfigConstants.JOB_MANAGER_WEB_ACCESS_FILE_KEY, null);
		config.put(ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT);
		config.put(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, user.getString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, "") + "/log");
		config.put(ConfigConstants.WEB_FRONTEND_PORT_KEY, ConfigConstants.DEFAULT_WEBCLIENT_PORT);
		config.put(ConfigConstants.WEB_TMP_DIR_KEY, ConfigConstants.DEFAULT_WEB_TMP_DIR);
		config.put(ConfigConstants.WEB_JOB_UPLOAD_DIR_KEY, ConfigConstants.DEFAULT_WEB_JOB_STORAGE_DIR);
		config.put(ConfigConstants.WEB_PLAN_DUMP_DIR_KEY, ConfigConstants.DEFAULT_WEB_PLAN_DUMP_DIR);
		config.put(ConfigConstants.WEB_ACCESS_FILE_KEY, ConfigConstants.DEFAULT_WEB_ACCESS_FILE_PATH);
		config.put(ConfigConstants.AKKA_ASK_TIMEOUT, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);
		Duration askTimeout = Duration.apply(user.getString(ConfigConstants.AKKA_ASK_TIMEOUT, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT));
		config.put(ConfigConstants.AKKA_STARTUP_TIMEOUT, askTimeout.toString());
		config.put(ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_INTERVAL, ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL);
		config.put(ConfigConstants.AKKA_TRANSPORT_HEARTBEAT_PAUSE, ConfigConstants.DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE);
		config.put(ConfigConstants.AKKA_TRANSPORT_THRESHOLD, ConfigConstants.DEFAULT_AKKA_TRANSPORT_THRESHOLD);
		config.put(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, askTimeout.div(10).toString());
		config.put(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, askTimeout.toString());
		config.put(ConfigConstants.AKKA_WATCH_THRESHOLD, ConfigConstants.DEFAULT_AKKA_WATCH_THRESHOLD);
		config.put(ConfigConstants.AKKA_TCP_TIMEOUT, askTimeout.toString());
		config.put(ConfigConstants.AKKA_FRAMESIZE, ConfigConstants.DEFAULT_AKKA_FRAMESIZE);
		config.put(ConfigConstants.AKKA_DISPATCHER_THROUGHPUT, ConfigConstants.DEFAULT_AKKA_DISPATCHER_THROUGHPUT);
		config.put(ConfigConstants.AKKA_LOG_LIFECYCLE_EVENTS, ConfigConstants.DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS);
		config.put(ConfigConstants.AKKA_LOOKUP_TIMEOUT, ConfigConstants.DEFAULT_AKKA_LOOKUP_TIMEOUT);
		config.put(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, "");
		config.put(ConfigConstants.FLINK_JVM_OPTIONS, "");
		config.put(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1);

		return config;
	}
}
