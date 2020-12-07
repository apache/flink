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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The helper class to deploy a table program on the cluster.
 */
public class ProgramDeployer {
	private static final Logger LOG = LoggerFactory.getLogger(ProgramDeployer.class);

	private final Configuration configuration;
	private final Pipeline pipeline;
	private final String jobName;
	private final ClassLoader userCodeClassloader;

	/**
	 * Deploys a table program on the cluster.
	 *
	 * @param configuration  the {@link Configuration} that is used for deployment
	 * @param jobName        job name of the Flink job to be submitted
	 * @param pipeline       Flink {@link Pipeline} to execute
	 */
	public ProgramDeployer(
			Configuration configuration,
			String jobName,
			Pipeline pipeline,
			ClassLoader userCodeClassloader) {
		this.configuration = configuration;
		this.pipeline = pipeline;
		this.jobName = jobName;
		this.userCodeClassloader = userCodeClassloader;
	}

	public CompletableFuture<JobClient> deploy() {
		LOG.info("Submitting job {} for query {}`", pipeline, jobName);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Submitting job {} with configuration: \n{}", pipeline, configuration);
		}

		if (configuration.get(DeploymentOptions.TARGET) == null) {
			throw new RuntimeException("No execution.target specified in your configuration file.");
		}

		PipelineExecutorServiceLoader executorServiceLoader = new DefaultExecutorServiceLoader();
		final PipelineExecutorFactory executorFactory;
		try {
			executorFactory = executorServiceLoader.getExecutorFactory(configuration);
		} catch (Exception e) {
			throw new RuntimeException("Could not retrieve ExecutorFactory.", e);
		}

		final PipelineExecutor executor = executorFactory.getExecutor(configuration);
		CompletableFuture<JobClient> jobClient;
		try {
			jobClient = executor.execute(pipeline, configuration, userCodeClassloader);
		} catch (Exception e) {
			throw new RuntimeException("Could not execute program.", e);
		}
		return jobClient;
	}
}

