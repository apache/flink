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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.ExecutorServiceLoader;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The factory that instantiates the environment to be used when running jobs that are
 * submitted through a pre-configured client connection.
 * This happens for example when a job is submitted from the command line.
 */
public class ContextEnvironmentFactory implements ExecutionEnvironmentFactory {

	private final ExecutorServiceLoader executorServiceLoader;

	private final Configuration configuration;

	private final ClassLoader userCodeClassLoader;

	public ContextEnvironmentFactory(
			final ExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userCodeClassLoader) {
		this.executorServiceLoader = checkNotNull(executorServiceLoader);
		this.configuration = checkNotNull(configuration);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
	}

	@Override
	public ExecutionEnvironment createExecutionEnvironment() {
		return new ContextEnvironment(
				executorServiceLoader,
				configuration,
				userCodeClassLoader);
	}
}
