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

package org.apache.flink.table.client.config;

/**
 * Strings used for key and values in an environment file.
 */
public final class PropertyStrings {

	private PropertyStrings() {
		// private
	}

	public static final String EXECUTION = "execution";

	public static final String EXECUTION_TYPE = "type";

	public static final String EXECUTION_TYPE_VALUE_STREAMING = "streaming";

	public static final String EXECUTION_TYPE_VALUE_BATCH = "batch";

	public static final String EXECUTION_TIME_CHARACTERISTIC = "time-characteristic";

	public static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_EVENT_TIME = "event-time";

	public static final String EXECUTION_TIME_CHARACTERISTIC_VALUE_PROCESSING_TIME = "processing-time";

	public static final String EXECUTION_PERIODIC_WATERMARKS_INTERVAL = "periodic-watermarks-interval";

	public static final String EXECUTION_MIN_STATE_RETENTION = "min-idle-state-retention";

	public static final String EXECUTION_MAX_STATE_RETENTION = "max-idle-state-retention";

	public static final String EXECUTION_PARALLELISM = "parallelism";

	public static final String EXECUTION_MAX_PARALLELISM = "max-parallelism";

	public static final String EXECUTION_RESULT_MODE = "result-mode";

	public static final String EXECUTION_RESULT_MODE_VALUE_CHANGELOG = "changelog";

	public static final String EXECUTION_RESULT_MODE_VALUE_TABLE = "table";

	public static final String DEPLOYMENT = "deployment";

	public static final String DEPLOYMENT_TYPE = "type";

	public static final String DEPLOYMENT_TYPE_VALUE_STANDALONE = "standalone";

	public static final String DEPLOYMENT_RESPONSE_TIMEOUT = "response-timeout";

	public static final String DEPLOYMENT_GATEWAY_ADDRESS = "gateway-address";

	public static final String DEPLOYMENT_GATEWAY_PORT = "gateway-port";
}
