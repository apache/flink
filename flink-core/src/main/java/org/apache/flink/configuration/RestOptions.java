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

import org.apache.flink.annotation.Internal;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration parameters for REST communication.
 */
@Internal
public class RestOptions {

	/**
	 * The address that the server binds itself to / the client connects to.
	 */
	public static final ConfigOption<String> REST_ADDRESS =
		key("rest.address")
			.defaultValue("localhost")
			.withDescription("The address that the server binds itself to / the client connects to.");

	/**
	 * The port that the server listens on / the client connects to.
	 */
	public static final ConfigOption<Integer> REST_PORT =
		key("rest.port")
			.defaultValue(9067)
			.withDescription("The port that the server listens on / the client connects to.");

	/**
	 * The time in ms that the client waits for the leader address, e.g., Dispatcher or
	 * WebMonitorEndpoint.
	 */
	public static final ConfigOption<Long> AWAIT_LEADER_TIMEOUT =
		key("rest.await-leader-timeout")
			.defaultValue(30_000L)
			.withDescription("The time in ms that the client waits for the leader address, e.g., " +
				"Dispatcher or WebMonitorEndpoint");

	/**
	 * The number of retries the client will attempt if a retryable operations fails.
	 * @see #RETRY_DELAY
	 */
	public static final ConfigOption<Integer> RETRY_MAX_ATTEMPTS =
		key("rest.retry.max-attempts")
			.defaultValue(20)
			.withDescription("The number of retries the client will attempt if a retryable " +
				"operations fails.");

	/**
	 * The time in ms that the client waits between retries.
	 * @see #RETRY_MAX_ATTEMPTS
	 */
	public static final ConfigOption<Long> RETRY_DELAY =
		key("rest.retry.delay")
			.defaultValue(3_000L)
			.withDescription(String.format("The time in ms that the client waits between retries " +
				"(See also `%s`).", RETRY_MAX_ATTEMPTS.key()));

	/**
	 * The maximum time in ms for the client to establish a TCP connection.
	 */
	public static final ConfigOption<Long> CONNECTION_TIMEOUT =
		key("rest.connection-timeout")
			.defaultValue(15_000L)
			.withDescription("The maximum time in ms for the client to establish a TCP connection.");
}
