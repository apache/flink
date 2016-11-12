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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import java.net.InetSocketAddress;

import static org.apache.flink.client.CliFrontend.setJobManagerAddressInConfig;

/**
 * The default CLI which is used for interaction with standalone clusters.
 */
public class DefaultCLI implements CustomCommandLine<StandaloneClusterClient> {

	@Override
	public boolean isActive(CommandLine commandLine, Configuration configuration) {
		// always active because we can try to read a JobManager address from the config
		return true;
	}

	@Override
	public String getId() {
		return null;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
	}

	@Override
	public StandaloneClusterClient retrieveCluster(CommandLine commandLine, Configuration config) {

		if (commandLine.hasOption(CliFrontendParser.ADDRESS_OPTION.getOpt())) {
			String addressWithPort = commandLine.getOptionValue(CliFrontendParser.ADDRESS_OPTION.getOpt());
			InetSocketAddress jobManagerAddress = ClientUtils.parseHostPortAddress(addressWithPort);
			setJobManagerAddressInConfig(config, jobManagerAddress);
		}

		if (commandLine.hasOption(CliFrontendParser.ZOOKEEPER_NAMESPACE_OPTION.getOpt())) {
			String zkNamespace = commandLine.getOptionValue(CliFrontendParser.ZOOKEEPER_NAMESPACE_OPTION.getOpt());
			config.setString(ConfigConstants.ZOOKEEPER_NAMESPACE_KEY, zkNamespace);
		}

		StandaloneClusterDescriptor descriptor = new StandaloneClusterDescriptor(config);
		return descriptor.retrieve(null);
	}

	@Override
	public StandaloneClusterClient createCluster(
			String applicationName,
			CommandLine commandLine,
			Configuration config) throws UnsupportedOperationException {

		StandaloneClusterDescriptor descriptor = new StandaloneClusterDescriptor(config);
		return descriptor.deploy();
	}
}
