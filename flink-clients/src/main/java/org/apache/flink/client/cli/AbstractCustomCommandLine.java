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

import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.net.InetSocketAddress;

import static org.apache.flink.client.cli.CliFrontend.setJobManagerAddressInConfig;

/**
 * Base class for {@link CustomCommandLine} implementations which specify a JobManager address and
 * a ZooKeeper namespace.
 *
 * @param <C> type of the ClusterClient which is returned
 */
public abstract class AbstractCustomCommandLine<C extends ClusterClient> implements CustomCommandLine<C> {

	protected final Option zookeeperNamespaceOption = new Option("z", "zookeeperNamespace", true,
		"Namespace to create the Zookeeper sub-paths for high availability mode");


	protected final Option addressOption = new Option("m", "jobmanager", true,
		"Address of the JobManager (master) to which to connect. " +
			"Use this flag to connect to a different JobManager than the one specified in the configuration.");

	@Override
	public void addRunOptions(Options baseOptions) {
		// nothing to add here
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(addressOption);
		baseOptions.addOption(zookeeperNamespaceOption);
	}

	/**
	 * Override configuration settings by specified command line options.
	 *
	 * @param configuration to use as the base configuration
	 * @param commandLine containing the overriding values
	 * @return Effective configuration with the overriden configuration settings
	 */
	protected Configuration applyCommandLineOptionsToConfiguration(Configuration configuration, CommandLine commandLine) {
		final Configuration resultingConfiguration = new Configuration(configuration);

		if (commandLine.hasOption(addressOption.getOpt())) {
			String addressWithPort = commandLine.getOptionValue(addressOption.getOpt());
			InetSocketAddress jobManagerAddress = ClientUtils.parseHostPortAddress(addressWithPort);
			setJobManagerAddressInConfig(resultingConfiguration, jobManagerAddress);
		}

		if (commandLine.hasOption(zookeeperNamespaceOption.getOpt())) {
			String zkNamespace = commandLine.getOptionValue(zookeeperNamespaceOption.getOpt());
			resultingConfiguration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);
		}

		return resultingConfiguration;
	}
}
