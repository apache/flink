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

	// ------------------------------------------------------------------------
	//  process parameters
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> FLINK_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts")
		.defaultValue("");

	public static final ConfigOption<String> FLINK_JM_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.jobmanager")
		.defaultValue("");

	public static final ConfigOption<String> FLINK_TM_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.taskmanager")
		.defaultValue("");

	// ------------------------------------------------------------------------
	//  program
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> DEFAULT_PARALLELISM_KEY = ConfigOptions
		.key("parallelism.default")
		.defaultValue(-1);

	// ------------------------------------------------------------------------
	//  checkpoints / fault tolerance
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> STATE_BACKEND = ConfigOptions
		.key("state.backend")
		.noDefaultValue();

	/** The maximum number of completed checkpoint instances to retain.*/
	public static final ConfigOption<Integer> MAX_RETAINED_CHECKPOINTS = ConfigOptions
		.key("state.checkpoints.num-retained")
		.defaultValue(1);

	/** The default directory for savepoints. */
	public static final ConfigOption<String> SAVEPOINT_DIRECTORY = ConfigOptions
		.key("state.savepoints.dir")
		.noDefaultValue()
		.withDeprecatedKeys("savepoints.state.backend.fs.dir");

	/** The default directory used for persistent checkpoints. */
	public static final ConfigOption<String> CHECKPOINTS_DIRECTORY = ConfigOptions
		.key("state.checkpoints.dir")
		.noDefaultValue();
}
