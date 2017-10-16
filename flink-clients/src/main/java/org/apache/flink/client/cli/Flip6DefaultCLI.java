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
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.Flip6StandaloneClusterDescriptor;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;

import static org.apache.flink.client.CliFrontend.setJobManagerAddressInConfig;

/**
 * The default CLI which is used for interaction with standalone clusters.
 */
public class Flip6DefaultCLI implements CustomCommandLine<RestClusterClient> {

	public static final Option FLIP_6 = new Option("flip6", "Switches the client to Flip-6 mode.");

	static {
		FLIP_6.setRequired(false);
	}

	@Override
	public boolean isActive(CommandLine commandLine, Configuration configuration) {
		return commandLine.hasOption(FLIP_6.getOpt());
	}

	@Override
	public String getId() {
		return "flip6";
	}

	@Override
	public void addRunOptions(Options baseOptions) {
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(FLIP_6);
	}

	@Override
	public RestClusterClient retrieveCluster(CommandLine commandLine, Configuration config, String configurationDirectory) {
		if (commandLine.hasOption(CliFrontendParser.ADDRESS_OPTION.getOpt())) {
			String addressWithPort = commandLine.getOptionValue(CliFrontendParser.ADDRESS_OPTION.getOpt());
			InetSocketAddress jobManagerAddress = ClientUtils.parseHostPortAddress(addressWithPort);
			setJobManagerAddressInConfig(config, jobManagerAddress);
		}

		if (commandLine.hasOption(CliFrontendParser.ZOOKEEPER_NAMESPACE_OPTION.getOpt())) {
			String zkNamespace = commandLine.getOptionValue(CliFrontendParser.ZOOKEEPER_NAMESPACE_OPTION.getOpt());
			config.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);
		}

		Flip6StandaloneClusterDescriptor descriptor = new Flip6StandaloneClusterDescriptor(config);
		return descriptor.retrieve(null);
	}

	@Override
	public RestClusterClient createCluster(
			String applicationName,
			CommandLine commandLine,
			Configuration config,
			String configurationDirectory,
			List<URL> userJarFiles) throws UnsupportedOperationException {

		Flip6StandaloneClusterDescriptor descriptor = new Flip6StandaloneClusterDescriptor(config);
		ClusterSpecification clusterSpecification = ClusterSpecification.fromConfiguration(config);

		return descriptor.deploySessionCluster(clusterSpecification);
	}
}
