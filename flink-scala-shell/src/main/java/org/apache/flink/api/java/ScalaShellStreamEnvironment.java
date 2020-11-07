/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.JarUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamExecutionEnvironment} for the Scala shell.
 */
@Internal
public class ScalaShellStreamEnvironment extends StreamExecutionEnvironment {

	/** The jar files that need to be attached to each job. */
	private final List<URL> jarFiles;

	/** reference to Scala Shell, for access to virtual directory. */
	private final FlinkILoop flinkILoop;

	public ScalaShellStreamEnvironment(
			final Configuration configuration,
			final FlinkILoop flinkILoop,
			final String... jarFiles) {

		super(validateAndGetConfiguration(configuration));
		this.flinkILoop = checkNotNull(flinkILoop);
		this.jarFiles = checkNotNull(JarUtils.getJarFiles(jarFiles));
	}

	private static Configuration validateAndGetConfiguration(final Configuration configuration) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The RemoteEnvironment cannot be used when submitting a program through a client, " +
							"or running in a TestEnvironment context.");
		}
		return checkNotNull(configuration);
	}

	@Override
	public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
		updateDependencies();
		return super.executeAsync(streamGraph);
	}

	private void updateDependencies() throws Exception {
		final Configuration configuration = getConfiguration();
		checkState(configuration.getBoolean(DeploymentOptions.ATTACHED), "Only ATTACHED mode is supported by the scala shell.");

		final List<URL> updatedJarFiles = getUpdatedJarFiles();
		ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, updatedJarFiles, URL::toString);
	}

	public Configuration getClientConfiguration() {
		return getConfiguration();
	}

	private List<URL> getUpdatedJarFiles() throws MalformedURLException {
		final URL jarUrl = flinkILoop.writeFilesToDisk().getAbsoluteFile().toURI().toURL();
		final List<URL> allJarFiles = new ArrayList<>(jarFiles);
		allJarFiles.add(jarUrl);
		return allJarFiles;
	}

	public static void disableAllContextAndOtherEnvironments() {
		initializeContextEnvironment(configuration -> {
			throw new UnsupportedOperationException("Execution Environment is already defined for this shell.");
		});
	}

	public static void resetContextEnvironments() {
		StreamExecutionEnvironment.resetContextEnvironment();
	}
}
