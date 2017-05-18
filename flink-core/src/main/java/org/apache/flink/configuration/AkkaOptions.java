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
 * Akka configuration options.
 *
 * TODO: Migrate other akka config options to this file
 */
@PublicEvolving
public class AkkaOptions {

	public static String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL = "1000 s";

	public static String DEFAULT_AKKA_TCP_TIMEOUT = "20 s";

	public static String DEFAULT_AKKA_WATCH_HEARTBEAT_INTERVAL = "10 s";

	public static String DEFAULT_AKKA_WATCH_HEARTBEAT_PAUSE = "60 s";

	public static String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE = "6000 s";

	public static double DEFAULT_AKKA_TRANSPORT_THRESHOLD = 300.0;

	public static int DEFAULT_AKKA_WATCH_THRESHOLD = 12;

	public static int DEFAULT_AKKA_DISPATCHER_THROUGHPUT = 15;

	public static boolean DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS = false;

	public static String DEFAULT_AKKA_FRAMESIZE = "10485760b";

	public static String DEFAULT_AKKA_ASK_TIMEOUT = "10 s";

	public static String DEFAULT_AKKA_LOOKUP_TIMEOUT = "10 s";

	public static String DEFAULT_AKKA_CLIENT_TIMEOUT = "60 s";

	public static boolean DEFAULT_AKKA_SSL_ENABLED = true;

	public static boolean DEFAULT_AKKA_JVM_EXIT_ON_FATAL_ERROR = true;

	/**
	 * Timeout for akka ask calls
	 */
	public static final ConfigOption<String> AKKA_ASK_TIMEOUT = ConfigOptions
		.key("akka.ask.timeout")
		.defaultValue(DEFAULT_AKKA_ASK_TIMEOUT);

	/**
	 * The Akka tcp connection timeout.
	 */
	public static final ConfigOption<String> AKKA_TCP_TIMEOUT = ConfigOptions
		.key("akka.tcp.timeout")
		.defaultValue(DEFAULT_AKKA_TCP_TIMEOUT);

	/**
	 * The Akka death watch heartbeat interval.
	 */
	public static final ConfigOption<String> AKKA_WATCH_HEARTBEAT_INTERVAL = ConfigOptions
		.key("akka.watch.heartbeat.interval")
		.defaultValue(DEFAULT_AKKA_WATCH_HEARTBEAT_INTERVAL);

	/**
	 * The maximum acceptable Akka death watch heartbeat pause.
	 */
	public static final ConfigOption<String> AKKA_WATCH_HEARTBEAT_PAUSE = ConfigOptions
		.key("akka.watch.heartbeat.pause")
		.defaultValue(DEFAULT_AKKA_WATCH_HEARTBEAT_PAUSE);

	/**
	 * Timeout for the startup of the actor system
	 */
	public static final String AKKA_STARTUP_TIMEOUT = "akka.startup-timeout";

	/**
	 * Heartbeat interval of the transport failure detector
	 */
	public static final ConfigOption<String> AKKA_TRANSPORT_HEARTBEAT_INTERVAL = ConfigOptions
		.key("akka.transport.heartbeat.interval")
		.defaultValue(DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL);

	/**
	 * Allowed heartbeat pause for the transport failure detector
	 */
	public static final ConfigOption<String> AKKA_TRANSPORT_HEARTBEAT_PAUSE = ConfigOptions
		.key("akka.transport.heartbeat.pause")
		.defaultValue(DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE);

	/**
	 * Detection threshold of transport failure detector
	 */
	public static final ConfigOption<Double> AKKA_TRANSPORT_THRESHOLD = ConfigOptions
		.key("akka.transport.threshold")
		.defaultValue(DEFAULT_AKKA_TRANSPORT_THRESHOLD);

	/**
	 * Detection threshold for the phi accrual watch failure detector
	 */
	public static final ConfigOption<Integer> AKKA_WATCH_THRESHOLD = ConfigOptions
		.key("akka.watch.threshold")
		.defaultValue(DEFAULT_AKKA_WATCH_THRESHOLD);

	/**
	 * Override SSL support for the Akka transport
	 */
	public static final ConfigOption<Boolean> AKKA_SSL_ENABLED = ConfigOptions
		.key("akka.ssl.enabled")
		.defaultValue(DEFAULT_AKKA_SSL_ENABLED);

	/**
	 * Maximum framesize of akka messages
	 */
	public static final ConfigOption<String> AKKA_FRAMESIZE = ConfigOptions
		.key("akka.framesize")
		.defaultValue(DEFAULT_AKKA_FRAMESIZE);

	/**
	 * Maximum number of messages until another actor is executed by the same thread
	 */
	public static final ConfigOption<Integer> AKKA_DISPATCHER_THROUGHPUT = ConfigOptions
		.key("akka.throughput")
		.defaultValue(DEFAULT_AKKA_DISPATCHER_THROUGHPUT);

	/**
	 * Log lifecycle events
	 */
	public static final ConfigOption<Boolean> AKKA_LOG_LIFECYCLE_EVENTS = ConfigOptions
		.key("akka.log.lifecycle.events")
		.defaultValue(DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS);

	/**
	 * Timeout for all blocking calls that look up remote actors
	 */
	public static final ConfigOption<String> AKKA_LOOKUP_TIMEOUT = ConfigOptions
		.key("akka.lookup.timeout")
		.defaultValue(DEFAULT_AKKA_LOOKUP_TIMEOUT);

	/**
	 * Timeout for all blocking calls on the client side
	 */
	public static final ConfigOption<String> AKKA_CLIENT_TIMEOUT = ConfigOptions
		.key("akka.client.timeout")
		.defaultValue(DEFAULT_AKKA_CLIENT_TIMEOUT);

	/**
	 * Exit JVM on fatal Akka errors
	 */
	public static final ConfigOption<Boolean> AKKA_JVM_EXIT_ON_FATAL_ERROR = ConfigOptions
		.key("akka.jvm-exit-on-fatal-error")
		.defaultValue(DEFAULT_AKKA_JVM_EXIT_ON_FATAL_ERROR);
}
