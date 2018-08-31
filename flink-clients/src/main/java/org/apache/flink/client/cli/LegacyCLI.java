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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.LegacyStandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;

import javax.annotation.Nullable;

/**
 * The CLI which is used for interaction with the legacy standalone clusters.
 */
public class LegacyCLI extends AbstractCustomCommandLine<StandaloneClusterId> {

	public LegacyCLI(Configuration configuration) {
		super(configuration);
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		// always active because we can try to read a JobManager address from the config
		return true;
	}

	@Override
	public String getId() {
		return "legacy";
	}

	@Override
	public LegacyStandaloneClusterDescriptor createClusterDescriptor(
			CommandLine commandLine) throws FlinkException {
		final Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);

		return new LegacyStandaloneClusterDescriptor(effectiveConfiguration);
	}

	@Override
	@Nullable
	public StandaloneClusterId getClusterId(CommandLine commandLine) {
		return StandaloneClusterId.getInstance();
	}

	@Override
	public ClusterSpecification getClusterSpecification(CommandLine commandLine) {
		return new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();
	}
}
