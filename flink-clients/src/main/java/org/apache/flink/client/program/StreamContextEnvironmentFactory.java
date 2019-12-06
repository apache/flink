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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.ExecutorServiceLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The factory that instantiates the environment to be used when running jobs that are
 * submitted through a pre-configured client connection.
 * This happens for example when a job is submitted from the command line.
 */
public class StreamContextEnvironmentFactory implements StreamExecutionEnvironmentFactory {

	private final ExecutorServiceLoader executorServiceLoader;

	private final Configuration configuration;

	private final ClassLoader userCodeClassLoader;

	private final AtomicReference<JobExecutionResult> jobExecutionResult;

	private boolean alreadyCalled;

	public StreamContextEnvironmentFactory(
			final ExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userCodeClassLoader,
			final AtomicReference<JobExecutionResult> jobExecutionResult) {
		this.executorServiceLoader = checkNotNull(executorServiceLoader);
		this.configuration = checkNotNull(configuration);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		this.jobExecutionResult = checkNotNull(jobExecutionResult);

		this.alreadyCalled = false;
	}

	@Override
	public StreamExecutionEnvironment createExecutionEnvironment() {
		verifyCreateIsCalledOnceWhenInDetachedMode();
		return new StreamContextEnvironment(
				executorServiceLoader,
				configuration,
				userCodeClassLoader,
				jobExecutionResult);
	}

	private void verifyCreateIsCalledOnceWhenInDetachedMode() {
		if (!configuration.getBoolean(DeploymentOptions.ATTACHED) && alreadyCalled) {
			throw new InvalidProgramException("Multiple environments cannot be created in detached mode");
		}
		alreadyCalled = true;
	}
}
