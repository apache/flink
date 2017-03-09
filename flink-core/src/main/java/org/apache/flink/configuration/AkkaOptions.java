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
	 * Timeout for akka ask calls
	 */
	public static final ConfigOption<String> AKKA_ASK_TIMEOUT = ConfigOptions
		.key("akka.ask.timeout")
		.defaultValue("10 s");

	/**
	 * The Akka tcp connection timeout.
	 */
	public static final ConfigOption<String> AKKA_TCP_TIMEOUT = ConfigOptions
		.key("akka.tcp.timeout")
		.defaultValue("20 s");

	/**
	 * The Akka death watch heartbeat interval.
	 */
	public static final ConfigOption<String> AKKA_WATCH_HEARTBEAT_INTERVAL = ConfigOptions
		.key("akka.watch.heartbeat.interval")
		.defaultValue("10 s");

	/**
	 * The maximum acceptable Akka death watch heartbeat pause.
	 */
	public static final ConfigOption<String> AKKA_WATCH_HEARTBEAT_PAUSE = ConfigOptions
		.key("akka.watch.heartbeat.pause")
		.defaultValue("60 s");
}
