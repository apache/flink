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

import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;

/**
 * Base class for {@link CustomCommandLine} implementations which specify a JobManager address and
 * a ZooKeeper namespace.
 *
 */
public abstract class AbstractCustomCommandLine implements CustomCommandLine {

	protected final Option zookeeperNamespaceOption = new Option("z", "zookeeperNamespace", true,
		"Namespace to create the Zookeeper sub-paths for high availability mode");

	protected final Configuration configuration;

	protected AbstractCustomCommandLine(Configuration configuration) {
		this.configuration = new UnmodifiableConfiguration(Preconditions.checkNotNull(configuration));
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		// nothing to add here
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(zookeeperNamespaceOption);
	}

	@Override
	public Configuration applyCommandLineOptionsToConfiguration(CommandLine commandLine) throws FlinkException {
		final Configuration resultingConfiguration = new Configuration(configuration);
		resultingConfiguration.setString(DeploymentOptions.TARGET, RemoteExecutor.NAME);

		if (commandLine.hasOption(zookeeperNamespaceOption.getOpt())) {
			String zkNamespace = commandLine.getOptionValue(zookeeperNamespaceOption.getOpt());
			resultingConfiguration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);
		}

		return resultingConfiguration;
	}

	protected void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);

		formatter.setSyntaxPrefix("   Optional");
		Options options = new Options();
		addGeneralOptions(options);
		addRunOptions(options);
		formatter.printHelp(" ", options);
	}

	public static int handleCliArgsException(CliArgsException e, Logger logger) {
		logger.error("Could not parse the command line arguments.", e);

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	public static int handleError(Throwable t, Logger logger) {
		logger.error("Error while running the Flink session.", t);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		t.printStackTrace();
		return 1;
	}
}
