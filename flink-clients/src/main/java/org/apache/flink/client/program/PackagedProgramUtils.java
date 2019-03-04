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
import org.apache.flink.api.common.Plan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nullable;

import java.net.URISyntaxException;
import java.net.URL;

/**
 * Utility class for {@link PackagedProgram} related operations.
 */
public class PackagedProgramUtils {

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
		Thread.currentThread().setContextClassLoader(packagedProgram.getUserCodeClassLoader());
		final Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), configuration);
		final FlinkPlan flinkPlan;

		if (packagedProgram.isUsingProgramEntryPoint()) {

			final JobWithJars jobWithJars = packagedProgram.getPlanWithJars();

			final Plan plan = jobWithJars.getPlan();

			if (plan.getDefaultParallelism() <= 0) {
				plan.setDefaultParallelism(defaultParallelism);
			}

			flinkPlan = optimizer.compile(jobWithJars.getPlan());
		} else if (packagedProgram.isUsingInteractiveMode()) {
			final OptimizerPlanEnvironment optimizerPlanEnvironment = new OptimizerPlanEnvironment(optimizer);

			optimizerPlanEnvironment.setParallelism(defaultParallelism);

			flinkPlan = optimizerPlanEnvironment.getOptimizedPlan(packagedProgram);
		} else {
			throw new ProgramInvocationException("PackagedProgram does not have a valid invocation mode.");
		}

		final JobGraph jobGraph;

		if (flinkPlan instanceof StreamingPlan) {
			jobGraph = ((StreamingPlan) flinkPlan).getJobGraph(jobID);
			jobGraph.setSavepointRestoreSettings(packagedProgram.getSavepointSettings());
		} else {
			final JobGraphGenerator jobGraphGenerator = new JobGraphGenerator(configuration);
			jobGraph = jobGraphGenerator.compileJobGraph((OptimizedPlan) flinkPlan, jobID);
		}

		for (URL url : packagedProgram.getAllLibraries()) {
			try {
				jobGraph.addJar(new Path(url.toURI()));
			} catch (URISyntaxException e) {
				throw new ProgramInvocationException("Invalid URL for jar file: " + url + '.', jobGraph.getJobID(), e);
			}
		}

		jobGraph.setClasspaths(packagedProgram.getClasspaths());

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

	private PackagedProgramUtils() {}
}
