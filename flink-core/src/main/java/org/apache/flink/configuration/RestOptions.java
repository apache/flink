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
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configuration parameters for REST communication.
 */
@Internal
public class RestOptions {

	private static final String REST_PORT_KEY = "rest.port";

	/**
	 * The address that the server binds itself to.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
	public static final ConfigOption<String> BIND_ADDRESS =
		key("rest.bind-address")
			.noDefaultValue()
			.withFallbackKeys(WebOptions.ADDRESS.key())
			.withDeprecatedKeys(ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_ADDRESS.key())
			.withDescription("The address that the server binds itself.");

	/**
	 * The port range that the server could bind itself to.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
	public static final ConfigOption<String> BIND_PORT =
		key("rest.bind-port")
			.defaultValue("8081")
			.withFallbackKeys(REST_PORT_KEY)
			.withDeprecatedKeys(WebOptions.PORT.key(), ConfigConstants.JOB_MANAGER_WEB_PORT_KEY)
			.withDescription("The port that the server binds itself. Accepts a list of ports (“50100,50101”), ranges" +
				" (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid" +
				" collisions when multiple Rest servers are running on the same machine.");


	/**
	 * The address that should be used by clients to connect to the server.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
	public static final ConfigOption<String> ADDRESS =
		key("rest.address")
			.noDefaultValue()
			.withFallbackKeys(JobManagerOptions.ADDRESS.key())
			.withDescription("The address that should be used by clients to connect to the server. Attention: This option is respected only if the high-availability configuration is NONE.");

	/**
	 * The port that the REST client connects to and the REST server binds to if {@link #BIND_PORT}
	 * has not been specified.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
	public static final ConfigOption<Integer> PORT =
		key(REST_PORT_KEY)
			.defaultValue(8081)
			.withDeprecatedKeys(WebOptions.PORT.key())
			.withDescription(
				Description.builder()
					.text("The port that the client connects to. If %s has not been specified, then the REST server will bind to this port. Attention: This option is respected only if the high-availability configuration is NONE.", text(BIND_PORT.key()))
					.build());

	/**
	 * The time in ms that the client waits for the leader address, e.g., Dispatcher or
	 * WebMonitorEndpoint.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Long> AWAIT_LEADER_TIMEOUT =
		key("rest.await-leader-timeout")
			.defaultValue(30_000L)
			.withDescription("The time in ms that the client waits for the leader address, e.g., " +
				"Dispatcher or WebMonitorEndpoint");

	/**
	 * The number of retries the client will attempt if a retryable operations fails.
	 * @see #RETRY_DELAY
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Integer> RETRY_MAX_ATTEMPTS =
		key("rest.retry.max-attempts")
			.defaultValue(20)
			.withDescription("The number of retries the client will attempt if a retryable " +
				"operations fails.");

	/**
	 * The time in ms that the client waits between retries.
	 * @see #RETRY_MAX_ATTEMPTS
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Long> RETRY_DELAY =
		key("rest.retry.delay")
			.defaultValue(3_000L)
			.withDescription(String.format("The time in ms that the client waits between retries " +
				"(See also `%s`).", RETRY_MAX_ATTEMPTS.key()));

	/**
	 * The maximum time in ms for the client to establish a TCP connection.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Long> CONNECTION_TIMEOUT =
		key("rest.connection-timeout")
			.defaultValue(15_000L)
			.withDescription("The maximum time in ms for the client to establish a TCP connection.");

	/**
	 * The maximum time in ms for a connection to stay idle before failing.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Long> IDLENESS_TIMEOUT =
		key("rest.idleness-timeout")
			.defaultValue(5L * 60L * 1_000L) // 5 minutes
			.withDescription("The maximum time in ms for a connection to stay idle before failing.");

	/**
	 * The maximum content length that the server will handle.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Integer> SERVER_MAX_CONTENT_LENGTH =
		key("rest.server.max-content-length")
			.defaultValue(104_857_600)
			.withDescription("The maximum content length in bytes that the server will handle.");

	/**
	 * The maximum content length that the client will handle.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Integer> CLIENT_MAX_CONTENT_LENGTH =
		key("rest.client.max-content-length")
			.defaultValue(104_857_600)
			.withDescription("The maximum content length in bytes that the client will handle.");

	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Integer> SERVER_NUM_THREADS =
		key("rest.server.numThreads")
			.defaultValue(4)
			.withDescription("The number of threads for the asynchronous processing of requests.");

	@Documentation.Section(Documentation.Sections.EXPERT_REST)
	public static final ConfigOption<Integer> SERVER_THREAD_PRIORITY = key("rest.server.thread-priority")
		.defaultValue(Thread.NORM_PRIORITY)
		.withDescription("Thread priority of the REST server's executor for processing asynchronous requests. " +
				"Lowering the thread priority will give Flink's main components more CPU time whereas " +
				"increasing will allocate more time for the REST server's processing.");
}
