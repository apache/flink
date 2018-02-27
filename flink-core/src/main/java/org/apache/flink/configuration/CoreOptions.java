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
 * The set of configuration options for core parameters.
 */
@PublicEvolving
public class CoreOptions {

	// ------------------------------------------------------------------------
	//  process parameters
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> CLASSLOADER_RESOLVE_ORDER = ConfigOptions
		.key("classloader.resolve-order")
		.defaultValue("child-first");

	public static final ConfigOption<String> ALWAYS_PARENT_FIRST_LOADER = ConfigOptions
		.key("classloader.parent-first-patterns")
		.defaultValue("java.;scala.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.slf4j;org.apache.log4j;org.apache.logging.log4j;org.apache.commons.logging;ch.qos.logback");


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

	// ------------------------------------------------------------------------
	//  file systems
	// ------------------------------------------------------------------------

	/**
	 * The default filesystem scheme, used for paths that do not declare a scheme explicitly.
	 */
	public static final ConfigOption<String> DEFAULT_FILESYSTEM_SCHEME = ConfigOptions
			.key("fs.default-scheme")
			.noDefaultValue();

	/**
	 * The total number of input plus output connections that a file system for the given scheme may open.
	 * Unlimited be default.
	 */
	public static ConfigOption<Integer> fileSystemConnectionLimit(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.total").defaultValue(-1);
	}

	/**
	 * The total number of input connections that a file system for the given scheme may open.
	 * Unlimited be default.
	 */
	public static ConfigOption<Integer> fileSystemConnectionLimitIn(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.input").defaultValue(-1);
	}

	/**
	 * The total number of output connections that a file system for the given scheme may open.
	 * Unlimited be default.
	 */
	public static ConfigOption<Integer> fileSystemConnectionLimitOut(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.output").defaultValue(-1);
	}

	/**
	 * If any connection limit is configured, this option can be optionally set to define after
	 * which time (in milliseconds) stream opening fails with a timeout exception, if no stream
	 * connection becomes available. Unlimited timeout be default.
	 */
	public static ConfigOption<Long> fileSystemConnectionLimitTimeout(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.timeout").defaultValue(0L);
	}

	/**
	 * If any connection limit is configured, this option can be optionally set to define after
	 * which time (in milliseconds) inactive streams are reclaimed. This option can help to prevent
	 * that inactive streams make up the full pool of limited connections, and no further connections
	 * can be established. Unlimited timeout be default.
	 */
	public static ConfigOption<Long> fileSystemConnectionLimitStreamInactivityTimeout(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.stream-timeout").defaultValue(0L);
	}
}
