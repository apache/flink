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
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.NetUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.net.InetSocketAddress;

import static org.apache.flink.client.cli.CliFrontend.setJobManagerAddressInConfig;

/**
 * The default CLI which is used for interaction with standalone clusters.
 */
public class DefaultCLI extends AbstractCustomCommandLine {

	public static final String ID = "default";

	private static final Option addressOption = new Option("m", "jobmanager", true,
		"Address of the JobManager to which to connect. " +
			"Use this flag to connect to a different JobManager than the one specified in the configuration. " +
			"Attention: This option is respected only if the high-availability configuration is NONE.");

	@Override
	public boolean isActive(CommandLine commandLine) {
		// always active because we can try to read a JobManager address from the config
		return true;
	}

	@Override
	public Configuration toConfiguration(CommandLine commandLine) throws FlinkException {

		final Configuration resultingConfiguration = super.toConfiguration(commandLine);
		if (commandLine.hasOption(addressOption.getOpt())) {
			String addressWithPort = commandLine.getOptionValue(addressOption.getOpt());
			InetSocketAddress jobManagerAddress = NetUtils.parseHostPortAddress(addressWithPort);
			setJobManagerAddressInConfig(resultingConfiguration, jobManagerAddress);
		}
		resultingConfiguration.setString(DeploymentOptions.TARGET, RemoteExecutor.NAME);

		DynamicPropertiesUtil.encodeDynamicProperties(commandLine, resultingConfiguration);

		return resultingConfiguration;
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		super.addGeneralOptions(baseOptions);
		baseOptions.addOption(addressOption);
		baseOptions.addOption(DynamicPropertiesUtil.DYNAMIC_PROPERTIES);
	}
}
