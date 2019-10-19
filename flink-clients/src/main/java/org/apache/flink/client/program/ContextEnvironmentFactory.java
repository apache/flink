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

package org.apache.flink.client.program;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The factory that instantiates the environment to be used when running jobs that are
 * submitted through a pre-configured client connection.
 * This happens for example when a job is submitted from the command line.
 */
public class ContextEnvironmentFactory implements ExecutionEnvironmentFactory {

	private final ClusterClient<?> client;

	private final List<URL> jarFilesToAttach;

	private final List<URL> classpathsToAttach;

	private final ClassLoader userCodeClassLoader;

	private final int defaultParallelism;

	private final boolean isDetached;

	private final SavepointRestoreSettings savepointSettings;

	private final AtomicReference<JobExecutionResult> jobExecutionResult;

	private boolean alreadyCalled;

	public ContextEnvironmentFactory(
		ClusterClient<?> client,
		List<URL> jarFilesToAttach,
		List<URL> classpathsToAttach,
		ClassLoader userCodeClassLoader,
		int defaultParallelism,
		boolean isDetached,
		SavepointRestoreSettings savepointSettings,
		AtomicReference<JobExecutionResult> jobExecutionResult) {
		this.client = client;
		this.jarFilesToAttach = jarFilesToAttach;
		this.classpathsToAttach = classpathsToAttach;
		this.userCodeClassLoader = userCodeClassLoader;
		this.defaultParallelism = defaultParallelism;
		this.isDetached = isDetached;
		this.savepointSettings = savepointSettings;
		this.alreadyCalled = false;
		this.jobExecutionResult = jobExecutionResult;
	}

	@Override
	public ExecutionEnvironment createExecutionEnvironment() {
		verifyCreateIsCalledOnceWhenInDetachedMode();

		final ContextEnvironment environment = new ContextEnvironment(
			client,
			jarFilesToAttach,
			classpathsToAttach,
			userCodeClassLoader,
			savepointSettings,
			isDetached,
			jobExecutionResult);
		if (defaultParallelism > 0) {
			environment.setParallelism(defaultParallelism);
		}
		return environment;
	}

	private void verifyCreateIsCalledOnceWhenInDetachedMode() {
		if (isDetached && alreadyCalled) {
			throw new InvalidProgramException("Multiple environments cannot be created in detached mode");
		}
		alreadyCalled = true;
	}
}
