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
 * The set of configuration options for core parameters.
 */
@PublicEvolving
public class CoreOptions {

	// ------------------------------------------------------------------------
	//  Classloading Parameters
	// ------------------------------------------------------------------------

	/**
	 * Defines the class resolution strategy when loading classes from user code,
	 * meaning whether to first check the user code jar ({@code "child-first"}) or
	 * the application classpath ({@code "parent-first"})
	 *
	 * <p>The default settings indicate to load classes first from the user code jar,
	 * which means that user code jars can include and load different dependencies than
	 * Flink uses (transitively).
	 *
	 * <p>Exceptions to the rules are defined via {@link #ALWAYS_PARENT_FIRST_LOADER}.
	 */
	public static final ConfigOption<String> CLASSLOADER_RESOLVE_ORDER = ConfigOptions
		.key("classloader.resolve-order")
		.defaultValue("child-first");

	/**
	 * The namespace patterns for classes that are loaded with a preference from the
	 * parent classloader, meaning the application class path, rather than any user code
	 * jar file. This option only has an effect when {@link #CLASSLOADER_RESOLVE_ORDER} is
	 * set to {@code "child-first"}.
	 *
	 * <p>It is important that all classes whose objects move between Flink's runtime and
	 * any user code (including Flink connectors that run as part of the user code) are
	 * covered by these patterns here. Otherwise it is be possible that the Flink runtime
	 * and the user code load two different copies of a class through the different class
	 * loaders. That leads to errors like "X cannot be cast to X" exceptions, where both
	 * class names are equal, or "X cannot be assigned to Y", where X should be a subclass
	 * of Y.
	 *
	 * <p>The following classes are loaded parent-first, to avoid any duplication:
	 * <ul>
	 *     <li>All core Java classes (java.*), because they must never be duplicated.</li>
	 *     <li>All core Scala classes (scala.*). Currently Scala is used in the Flink
	 *         runtime and in the user code, and some Scala classes cross the boundary,
	 *         such as the <i>FunctionX</i> classes. That may change if Scala eventually
	 *         lives purely as part of the user code.</li>
	 *     <li>All Flink classes (org.apache.flink.*). Note that this means that connectors
	 *         and formats (flink-avro, etc) are loaded parent-first as well if they are in the
	 *         core classpath.</li>
	 *     <li>Java annotations and loggers, defined by the following list:
	 *         javax.annotation;org.slf4j;org.apache.log4j;org.apache.logging.log4j;ch.qos.logback.
	 *         This is done for convenience, to avoid duplication of annotations and multiple
	 *         log bindings.</li>
	 * </ul>
	 */
	public static final ConfigOption<String> ALWAYS_PARENT_FIRST_LOADER = ConfigOptions
		.key("classloader.parent-first-patterns")
		.defaultValue("java.;scala.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.slf4j;org.apache.log4j;org.apache.logging.log4j;ch.qos.logback");

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
	//  generic io
	// ------------------------------------------------------------------------

	/**
	 * The config parameter defining the directories for temporary files, separated by
	 * ",", "|", or the system's {@link java.io.File#pathSeparator}.
	 */
	public static final ConfigOption<String> TMP_DIRS =
		key("io.tmp.dirs")
			.defaultValue(System.getProperty("java.io.tmpdir"))
			.withDeprecatedKeys("taskmanager.tmp.dirs");

	// ------------------------------------------------------------------------
	//  program
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> DEFAULT_PARALLELISM = ConfigOptions
		.key("parallelism.default")
		.defaultValue(1);

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
