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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nullable;

/**
 * Utility class for {@link PackagedProgram} related operations.
 */
public class PackagedProgramUtils {

	private static final String PYTHON_DRIVER_CLASS_NAME = "org.apache.flink.client.python.PythonDriver";

	private static final String PYTHON_GATEWAY_CLASS_NAME = "org.apache.flink.client.python.PythonGatewayServer";
	/**
	 * Creates a {@link JobGraph} with a specified {@link JobID}
	 * from the given {@link PackagedProgram}.
	 *
	 * @param packagedProgram to extract the JobGraph from
	 * @param configuration to use for the optimizer and job graph generator
	 * @param defaultParallelism for the JobGraph
	 * @param jobID the pre-generated job id
	 * @return JobGraph extracted from the PackagedProgram
	 * @throws ProgramInvocationException if the JobGraph generation failed
	 */
	public static JobGraph createJobGraph(
			PackagedProgram packagedProgram,
			Configuration configuration,
			int defaultParallelism,
			@Nullable JobID jobID) throws ProgramInvocationException {
		final Pipeline pipeline = getPipelineFromProgram(packagedProgram, defaultParallelism);
		final JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraph(pipeline, configuration, defaultParallelism);

		if (jobID != null) {
			jobGraph.setJobID(jobID);
		}
		jobGraph.addJars(packagedProgram.getJobJarAndDependencies());
		jobGraph.setClasspaths(packagedProgram.getClasspaths());
		jobGraph.setSavepointRestoreSettings(packagedProgram.getSavepointSettings());

		return jobGraph;
	}

	/**
	 * Creates a {@link JobGraph} with a random {@link JobID}
	 * from the given {@link PackagedProgram}.
	 *
	 * @param packagedProgram to extract the JobGraph from
	 * @param configuration to use for the optimizer and job graph generator
	 * @param defaultParallelism for the JobGraph
	 * @return JobGraph extracted from the PackagedProgram
	 * @throws ProgramInvocationException if the JobGraph generation failed
	 */
	public static JobGraph createJobGraph(
			PackagedProgram packagedProgram,
			Configuration configuration,
			int defaultParallelism) throws ProgramInvocationException {
		return createJobGraph(packagedProgram, configuration, defaultParallelism, null);
	}

	public static Pipeline getPipelineFromProgram(
			PackagedProgram prog,
			int parallelism) throws CompilerException, ProgramInvocationException {
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());

			// temporary hack to support the optimizer plan preview
			OptimizerPlanEnvironment env = new OptimizerPlanEnvironment();
			if (parallelism > 0) {
				env.setParallelism(parallelism);
			}
			return env.getPipeline(prog);
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	public static Boolean isPython(String entryPointClassName) {
		return (entryPointClassName != null) &&
			(entryPointClassName.equals(PYTHON_DRIVER_CLASS_NAME) || entryPointClassName.equals(PYTHON_GATEWAY_CLASS_NAME));
	}

	private PackagedProgramUtils() {}
}
