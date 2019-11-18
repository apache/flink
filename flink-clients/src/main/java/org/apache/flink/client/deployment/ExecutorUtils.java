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

package org.apache.flink.client.deployment;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class with method related to job execution.
 */
public class ExecutorUtils {

	/**
	 * Creates the {@link JobGraph} corresponding to the provided {@link Pipeline}.
	 *
	 * @param pipeline the pipeline whose job graph we are computing
	 * @param configuration the configuration with the necessary information such as jars and
	 *                         classpaths to be included, the parallelism of the job and potential
	 *                         savepoint settings used to bootstrap its state.
	 * @return the corresponding {@link JobGraph}.
	 */
	public static JobGraph getJobGraph(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) {
		checkNotNull(pipeline);
		checkNotNull(configuration);

		final ExecutionConfigAccessor executionConfigAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
		final JobGraph jobGraph = FlinkPipelineTranslationUtil
				.getJobGraph(pipeline, configuration, executionConfigAccessor.getParallelism());

		jobGraph.addJars(executionConfigAccessor.getJars());
		jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
		jobGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());

		return jobGraph;
	}
}
