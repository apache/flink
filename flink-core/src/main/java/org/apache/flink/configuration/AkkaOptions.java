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

	/**
	 * Timeout for akka ask calls.
	 */
	public static final ConfigOption<String> ASK_TIMEOUT = ConfigOptions
		.key("akka.ask.timeout")
		.defaultValue("10 s");

	/**
	 * The Akka death watch heartbeat interval.
	 */
	public static final ConfigOption<String> WATCH_HEARTBEAT_INTERVAL = ConfigOptions
		.key("akka.watch.heartbeat.interval")
		.defaultValue(ASK_TIMEOUT.defaultValue());

	/**
	 * The maximum acceptable Akka death watch heartbeat pause.
	 */
	public static final ConfigOption<String> WATCH_HEARTBEAT_PAUSE = ConfigOptions
		.key("akka.watch.heartbeat.pause")
		.defaultValue(ASK_TIMEOUT.defaultValue());

	/**
	 * The Akka tcp connection timeout.
	 */
	public static final ConfigOption<String> TCP_TIMEOUT = ConfigOptions
		.key("akka.tcp.timeout")
		.defaultValue("20 s");

	/**
	 * Timeout for the startup of the actor system.
	 */
	public static final ConfigOption<String> STARTUP_TIMEOUT = ConfigOptions
		.key("akka.startup-timeout")
		.noDefaultValue();

	/**
	 * Heartbeat interval of the transport failure detector.
	 */
	public static final ConfigOption<String> TRANSPORT_HEARTBEAT_INTERVAL = ConfigOptions
		.key("akka.transport.heartbeat.interval")
		.defaultValue("1000 s");

	/**
	 * Allowed heartbeat pause for the transport failure detector.
	 */
	public static final ConfigOption<String> TRANSPORT_HEARTBEAT_PAUSE = ConfigOptions
		.key("akka.transport.heartbeat.pause")
		.defaultValue("6000 s");

	/**
	 * Detection threshold of transport failure detector.
	 */
	public static final ConfigOption<Double> TRANSPORT_THRESHOLD = ConfigOptions
		.key("akka.transport.threshold")
		.defaultValue(300.0);

	/**
	 * Detection threshold for the phi accrual watch failure detector.
	 */
	public static final ConfigOption<Integer> WATCH_THRESHOLD = ConfigOptions
		.key("akka.watch.threshold")
		.defaultValue(12);

	/**
	 * Override SSL support for the Akka transport.
	 */
	public static final ConfigOption<Boolean> SSL_ENABLED = ConfigOptions
		.key("akka.ssl.enabled")
		.defaultValue(true);

	/**
	 * Maximum framesize of akka messages.
	 */
	public static final ConfigOption<String> FRAMESIZE = ConfigOptions
		.key("akka.framesize")
		.defaultValue("10485760b");

	/**
	 * Maximum number of messages until another actor is executed by the same thread.
	 */
	public static final ConfigOption<Integer> DISPATCHER_THROUGHPUT = ConfigOptions
		.key("akka.throughput")
		.defaultValue(15);

	/**
	 * Log lifecycle events.
	 */
	public static final ConfigOption<Boolean> LOG_LIFECYCLE_EVENTS = ConfigOptions
		.key("akka.log.lifecycle.events")
		.defaultValue(false);

	/**
	 * Timeout for all blocking calls that look up remote actors.
	 */
	public static final ConfigOption<String> LOOKUP_TIMEOUT = ConfigOptions
		.key("akka.lookup.timeout")
		.defaultValue("10 s");

	/**
	 * Timeout for all blocking calls on the client side.
	 */
	public static final ConfigOption<String> CLIENT_TIMEOUT = ConfigOptions
		.key("akka.client.timeout")
		.defaultValue("60 s");

	/**
	 * Exit JVM on fatal Akka errors.
	 */
	public static final ConfigOption<Boolean> JVM_EXIT_ON_FATAL_ERROR = ConfigOptions
		.key("akka.jvm-exit-on-fatal-error")
		.defaultValue(true);
}
