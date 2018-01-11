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

import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.Flip6StandaloneClusterDescriptor;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import javax.annotation.Nullable;

/**
 * The default CLI which is used for interaction with standalone clusters.
 */
public class Flip6DefaultCLI extends AbstractCustomCommandLine<RestClusterClient> {

	public static final Option FLIP_6 = new Option("flip6", "Switches the client to Flip-6 mode.");

	static {
		FLIP_6.setRequired(false);
	}

	public Flip6DefaultCLI(Configuration configuration) {
		super(configuration);
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		return commandLine.hasOption(FLIP_6.getOpt());
	}

	@Override
	public String getId() {
		return "flip6";
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		super.addGeneralOptions(baseOptions);
		baseOptions.addOption(FLIP_6);
	}

	@Override
	public ClusterDescriptor<RestClusterClient> createClusterDescriptor(
			CommandLine commandLine) throws FlinkException {
		final Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);

		return new Flip6StandaloneClusterDescriptor(effectiveConfiguration);
	}

	@Override
	@Nullable
	public String getClusterId(CommandLine commandLine) {
		return "flip6Standalone";
	}

	@Override
	public ClusterSpecification getClusterSpecification(CommandLine commandLine) {
		return new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();
	}
}
