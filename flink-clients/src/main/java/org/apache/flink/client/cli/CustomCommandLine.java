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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Custom command-line interface to load hooks for the command-line interface.
 */
public interface CustomCommandLine {

	/**
	 * Signals whether the custom command-line wants to execute or not.
	 * @param commandLine The command-line options
	 * @return True if the command-line wants to run, False otherwise
	 */
	boolean isActive(CommandLine commandLine);

	/**
	 * Gets the unique identifier of this CustomCommandLine.
	 * @return A unique identifier
	 */
	String getId();

	/**
	 * Adds custom options to the existing run options.
	 * @param baseOptions The existing options.
	 */
	void addRunOptions(Options baseOptions);

	/**
	 * Adds custom options to the existing general options.
	 *
	 * @param baseOptions The existing options.
	 */
	void addGeneralOptions(Options baseOptions);

	/**
	 * Materializes the command line arguments in the given {@link CommandLine} to a {@link
	 * Configuration} and returns it.
	 */
	Configuration toConfiguration(CommandLine commandLine) throws FlinkException;

	default CommandLine parseCommandLineOptions(String[] args, boolean stopAtNonOptions) throws CliArgsException {
		final Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);
		return CliFrontendParser.parse(options, args, stopAtNonOptions);
	}
}
