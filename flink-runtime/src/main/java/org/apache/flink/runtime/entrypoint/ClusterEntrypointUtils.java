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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Optional;

/**
 * Utility class for {@link org.apache.flink.runtime.entrypoint.ClusterEntrypoint}.
 */
public final class ClusterEntrypointUtils {

	protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypointUtils.class);

	private ClusterEntrypointUtils() {
		throw new UnsupportedOperationException("This class should not be instantiated.");
	}

	/**
	 * Parses passed String array using the parameter definitions of the passed {@code ParserResultFactory}.
	 * The method will call {@code System.exit} and print the usage information to stdout in case of a parsing error.
	 * @param args The String array that shall be parsed.
	 * @param parserResultFactory The {@code ParserResultFactory} that collects the parameter parsing instructions.
	 * @param mainClass The main class initiating the parameter parsing.
	 * @param <T> The parsing result type.
	 * @return The parsing result.
	 */
	public static <T> T parseParametersOrExit(String[] args, ParserResultFactory<T> parserResultFactory, Class<?> mainClass) {
		final CommandLineParser<T> commandLineParser = new CommandLineParser<>(parserResultFactory);

		try {
			return commandLineParser.parse(args);
		} catch (Exception e) {
			LOG.error("Could not parse command line arguments {}.", args, e);
			commandLineParser.printHelp(mainClass.getSimpleName());
			System.exit(ClusterEntrypoint.STARTUP_FAILURE_RETURN_CODE);
		}

		return null;
	}

	/**
	 * Tries to find the user library directory.
	 *
	 * @return the user library directory if it exits, returns {@link Optional#empty()} if there is none
	 */
	public static Optional<File> tryFindUserLibDirectory() {
		final File flinkHomeDirectory = deriveFlinkHomeDirectoryFromLibDirectory();
		final File usrLibDirectory = new File(flinkHomeDirectory, ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);

		if (!usrLibDirectory.isDirectory()) {
			return Optional.empty();
		}
		return Optional.of(usrLibDirectory);
	}

	@Nullable
	private static File deriveFlinkHomeDirectoryFromLibDirectory() {
		final String libDirectory = System.getenv().get(ConfigConstants.ENV_FLINK_LIB_DIR);

		if (libDirectory == null) {
			return null;
		} else {
			return new File(libDirectory).getParentFile();
		}
	}

	/**
	 * Gets and verify the io-executor pool size based on configuration.
	 *
	 * @param config The configuration to read.
	 * @return The legal io-executor pool size.
	 */
	public static int getPoolSize(Configuration config) {
		final int poolSize = config.getInteger(ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE, 4 * Hardware.getNumberCPUCores());
		Preconditions.checkArgument(poolSize > 0,
			String.format("Illegal pool size (%s) of io-executor, please re-configure '%s'.",
				poolSize, ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE.key()));
		return poolSize;
	}
}
