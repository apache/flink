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
 * The set of options relevant for all components
 */
@PublicEvolving
public class ClusterOptions {

	/**
	 * The heartbeat interval in milliseconds
	 */
	public static final ConfigOption<Long> HEARTBEAT_INTERVAL = ConfigOptions
		.key("heartbeat.interval")
		.defaultValue(5000L);

	/**
	 * The initial acceptable heartbeat pause for heartbeat responses in milliseconds
	 */
	public static final ConfigOption<Long> HEARTBEAT_INITIAL_ACCEPTABLE_PAUSE = ConfigOptions
		.key("heartbeat.initial-acceptable-pause")
		.defaultValue(20000L);

	/**
	 * The maximum acceptable heartbeat pause for heartbeat responses in milliseconds.
	 * The acceptable pause can be adapted when the heartbeat mechanism is detects that it got
	 * delayed, e.g. in case of garbage collection stalls. This options defines the maximum to
	 * which the heartbeat response pause can be extended.
	 *
	 */
	public static final ConfigOption<Long> HEARTBEAT_MAX_ACCEPTABLE_PAUSE = ConfigOptions
		.key("heartbeat.max-acceptable-pause")
		.defaultValue(60000L);
}
