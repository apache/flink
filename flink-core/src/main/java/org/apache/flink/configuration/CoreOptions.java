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

@PublicEvolving
public class CoreOptions {

	/**
	 * 
	 */
	public static final ConfigOption<String> FLINK_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts")
		.defaultValue("");

	public static final ConfigOption<String> FLINK_JM_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.jobmanager")
		.defaultValue("");

	public static final ConfigOption<String> FLINK_TM_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.taskmanager")
		.defaultValue("");

	public static final ConfigOption<Integer> DEFAULT_PARALLELISM_KEY = ConfigOptions
		.key("parallelism.default")
		.defaultValue(-1);
	
	public static final ConfigOption<String> STATE_BACKEND = ConfigOptions
		.key("state.backend")
		.noDefaultValue();

	/** The maximum number of completed checkpoint instances to retain.*/
	public static final ConfigOption<Integer> STATE_BACKEND_MAX_RETAINED_CHECKPOINTS_OPTIONS = ConfigOptions
		.key("state.checkpoints.max-retained-checkpoints")
		.defaultValue(1);
}
