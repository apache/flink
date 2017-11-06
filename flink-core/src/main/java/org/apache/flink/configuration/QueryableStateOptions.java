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

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to Queryable State.
 */
@PublicEvolving
public class QueryableStateOptions {

	// ------------------------------------------------------------------------
	// Server Options
	// ------------------------------------------------------------------------

	/**
	 * The config parameter defining the server port range of the queryable state proxy.
	 *
	 * <p>A proxy runs on each Task Manager, so many proxies may run on the same
	 * machine.
	 *
	 * <p>Given this, and to avoid port clashes, the user can specify a port range and
	 * the proxy will bind to the first free port in that range.
	 *
	 * <p>The specified range can be:
	 * <ol>
	 *     <li>a port: "9123",
	 *     <li>a range of ports: "50100-50200", or
	 *     <li>a list of ranges and or points: "50100-50200,50300-50400,51234"
	 * </ol>
	 *
	 * <p><b>The default port is 9069.</b>
	 */
	public static final ConfigOption<String> PROXY_PORT_RANGE =
			key("query.proxy.ports")
			.defaultValue("9069");

	/** Number of network (event loop) threads for the client proxy (0 => #slots). */
	public static final ConfigOption<Integer> PROXY_NETWORK_THREADS =
			key("query.proxy.network-threads")
					.defaultValue(0);

	/** Number of async query threads for the client proxy (0 => #slots). */
	public static final ConfigOption<Integer> PROXY_ASYNC_QUERY_THREADS =
			key("query.proxy.query-threads")
					.defaultValue(0);

	/**
	 * The config parameter defining the server port range of the queryable state server.
	 *
	 * <p>A state server runs on each Task Manager, so many server may run on the same
	 * machine.
	 *
	 * <p>Given this, and to avoid port clashes, the user can specify a port range and
	 * the server will bind to the first free port in that range.
	 *
	 * <p>The specified range can be:
	 * <ol>
	 *     <li>a port: "9123",
	 *     <li>a range of ports: "50100-50200", or
	 *     <li>a list of ranges and or points: "50100-50200,50300-50400,51234"
	 * </ol>
	 *
	 * <p><b>The default port is 9067.</b>
	 */
	public static final ConfigOption<String> SERVER_PORT_RANGE =
			key("query.server.ports")
			.defaultValue("9067");

	/** Number of network (event loop) threads for the KvState server (0 => #slots). */
	public static final ConfigOption<Integer> SERVER_NETWORK_THREADS =
			key("query.server.network-threads")
			.defaultValue(0);

	/** Number of async query threads for the KvStateServerHandler (0 => #slots). */
	public static final ConfigOption<Integer> SERVER_ASYNC_QUERY_THREADS =
			key("query.server.query-threads")
			.defaultValue(0);

	// ------------------------------------------------------------------------
	// Client Options
	// ------------------------------------------------------------------------

	/** Number of network (event loop) threads for the KvState client (0 => Use number of available cores). */
	public static final ConfigOption<Integer> CLIENT_NETWORK_THREADS =
			key("query.client.network-threads")
			.defaultValue(0);

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private QueryableStateOptions() {
	}
}
