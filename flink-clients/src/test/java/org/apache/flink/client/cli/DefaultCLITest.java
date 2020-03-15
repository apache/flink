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

import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URL;

import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link DefaultCLI}.
 */
public class DefaultCLITest extends CliFrontendTestBase {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that the configuration is properly passed via the DefaultCLI to the
	 * created ClusterDescriptor.
	 */
	@Test
	public void testConfigurationPassing() throws Exception {
		final Configuration configuration = getConfiguration();

		final String localhost = "localhost";
		final int port = 1234;

		configuration.setString(RestOptions.ADDRESS, localhost);
		configuration.setInteger(RestOptions.PORT, port);

		final AbstractCustomCommandLine defaultCLI = getCli(configuration);

		final String[] args = {};

		final CommandLine commandLine = defaultCLI.parseCommandLineOptions(args, false);
		final ClusterClient<?> clusterClient = getClusterClient(defaultCLI, commandLine);

		final URL webInterfaceUrl = new URL(clusterClient.getWebInterfaceURL());

		assertThat(webInterfaceUrl.getHost(), Matchers.equalTo(localhost));
		assertThat(webInterfaceUrl.getPort(), Matchers.equalTo(port));
	}

	/**
	 * Tests that command line options override the configuration settings.
	 */
	@Test
	public void testManualConfigurationOverride() throws Exception {
		final String localhost = "localhost";
		final int port = 1234;
		final Configuration configuration = getConfiguration();

		configuration.setString(JobManagerOptions.ADDRESS, localhost);
		configuration.setInteger(JobManagerOptions.PORT, port);

		final AbstractCustomCommandLine defaultCLI = getCli(configuration);

		final String manualHostname = "123.123.123.123";
		final int manualPort = 4321;
		final String[] args = {"-m", manualHostname + ':' + manualPort};

		final CommandLine commandLine = defaultCLI.parseCommandLineOptions(args, false);
		final ClusterClient<?> clusterClient = getClusterClient(defaultCLI, commandLine);

		final URL webInterfaceUrl = new URL(clusterClient.getWebInterfaceURL());

		assertThat(webInterfaceUrl.getHost(), Matchers.equalTo(manualHostname));
		assertThat(webInterfaceUrl.getPort(), Matchers.equalTo(manualPort));
	}

	private ClusterClient<?> getClusterClient(AbstractCustomCommandLine defaultCLI, CommandLine commandLine) throws FlinkException {
		final ClusterClientServiceLoader serviceLoader = new DefaultClusterClientServiceLoader();
		final Configuration executorConfig = defaultCLI.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<StandaloneClusterId> clusterFactory = serviceLoader.getClusterClientFactory(executorConfig);
		checkState(clusterFactory != null);

		final ClusterDescriptor<StandaloneClusterId> clusterDescriptor = clusterFactory.createClusterDescriptor(executorConfig);
		return clusterDescriptor.retrieve(clusterFactory.getClusterId(executorConfig)).getClusterClient();
	}
}
