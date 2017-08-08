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
@ConfigGroups(groups = {
	@ConfigGroup(name = "EnvironmentOptions", keyPrefix = "env")
})
public class CoreOptions {

	// ------------------------------------------------------------------------
	//  process parameters
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> FLINK_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts")
			.defaultValue("")
			.withDescription("Set custom JVM options. This value is respected by Flink's start scripts, both JobManager" +
				" and TaskManager, and Flink's YARN client. This can be used to set different garbage collectors or to" +
				" include remote debuggers into the JVMs running Flink's services. Enclosing options in double quotes" +
				" delays parameter substitution allowing access to variables from Flink's startup scripts." +
				"<p>Use `env.java.opts.jobmanager` and `env.java.opts.taskmanager` for JobManager or" +
				" TaskManager-specific options, respectively. </p>");

	public static final ConfigOption<String> FLINK_JM_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.jobmanager")
			.defaultValue("")
			.withDescription("JobManager-specific JVM options. These are used in addition to the regular" +
				" `" + FLINK_JVM_OPTIONS.key() + "`.");

	public static final ConfigOption<String> FLINK_TM_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.taskmanager")
			.defaultValue("")
			.withDescription("TaskManager-specific JVM options. These are used in addition to the regular" +
				" `" + FLINK_JVM_OPTIONS.key() + "`.");

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
