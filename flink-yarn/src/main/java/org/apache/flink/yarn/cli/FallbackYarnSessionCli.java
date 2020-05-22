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

package org.apache.flink.yarn.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.executors.YarnJobClusterExecutor;
import org.apache.flink.yarn.executors.YarnSessionClusterExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * A stub Yarn Command Line to throw an exception with the correct
 * message when the {@code HADOOP_CLASSPATH} is not set.
 */
@Internal
public class FallbackYarnSessionCli extends AbstractCustomCommandLine {

	public static final String ID = "yarn-cluster";

	private final Option applicationId;

	public FallbackYarnSessionCli(Configuration configuration) {
		super(configuration);
		applicationId = new Option("yid", "yarnapplicationId", true, "Attach to running YARN session");
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		super.addGeneralOptions(baseOptions);
		baseOptions.addOption(applicationId);
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		if (originalIsActive(commandLine)) {
			throw new IllegalStateException(YarnDeploymentTarget.ERROR_MESSAGE);
		}
		return false;
	}

	private boolean originalIsActive(CommandLine commandLine) {
		final String jobManagerOption = commandLine.getOptionValue(addressOption.getOpt(), null);
		final boolean yarnJobManager = ID.equals(jobManagerOption);
		final boolean hasYarnAppId = commandLine.hasOption(applicationId.getOpt())
				|| configuration.getOptional(YarnConfigOptions.APPLICATION_ID).isPresent();
		final boolean hasYarnExecutor = YarnSessionClusterExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET))
				|| YarnJobClusterExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
		return hasYarnExecutor || yarnJobManager || hasYarnAppId;
	}

	@Override
	public String getId() {
		return ID;
	}
}
