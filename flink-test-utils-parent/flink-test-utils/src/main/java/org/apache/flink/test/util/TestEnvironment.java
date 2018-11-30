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

package org.apache.flink.test.util;

import org.apache.flink.api.common.CodeAnalysisMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.JobExecutor;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.util.Preconditions;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link ExecutionEnvironment} implementation which executes its jobs on a
 * {@link MiniCluster}.
 */
public class TestEnvironment extends ExecutionEnvironment {

	private final JobExecutor jobExecutor;

	private final Collection<Path> jarFiles;

	private final Collection<URL> classPaths;

	private TestEnvironment lastEnv;

	public TestEnvironment(
			JobExecutor jobExecutor,
			int parallelism,
			boolean isObjectReuseEnabled,
			Collection<Path> jarFiles,
			Collection<URL> classPaths) {
		this.jobExecutor = Preconditions.checkNotNull(jobExecutor);
		this.jarFiles = Preconditions.checkNotNull(jarFiles);
		this.classPaths = Preconditions.checkNotNull(classPaths);

		setParallelism(parallelism);

		// disabled to improve build time
		getConfig().setCodeAnalysisMode(CodeAnalysisMode.DISABLE);

		if (isObjectReuseEnabled) {
			getConfig().enableObjectReuse();
		} else {
			getConfig().disableObjectReuse();
		}

		lastEnv = null;
	}

	public TestEnvironment(
			JobExecutor executor,
			int parallelism,
			boolean isObjectReuseEnabled) {
		this(
			executor,
			parallelism,
			isObjectReuseEnabled,
			Collections.emptyList(),
			Collections.emptyList());
	}

	@Override
	public JobExecutionResult getLastJobExecutionResult() {
		if (lastEnv == null) {
			return lastJobExecutionResult;
		}
		else {
			return lastEnv.getLastJobExecutionResult();
		}
	}

	@Override
	public void startNewSession() throws Exception {
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		OptimizedPlan op = compileProgram(jobName);

		JobGraphGenerator jgg = new JobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);

		for (Path jarFile: jarFiles) {
			jobGraph.addJar(jarFile);
		}

		jobGraph.setClasspaths(new ArrayList<>(classPaths));

		this.lastJobExecutionResult = jobExecutor.executeJobBlocking(jobGraph);
		return this.lastJobExecutionResult;
	}

	@Override
	public String getExecutionPlan() throws Exception {
		OptimizedPlan op = compileProgram("unused");

		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON(op);
	}

	private OptimizedPlan compileProgram(String jobName) {
		Plan p = createProgramPlan(jobName);

		Optimizer pc = new Optimizer(new DataStatistics(), new Configuration());
		return pc.compile(p);
	}

	public void setAsContext() {
		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				lastEnv = new TestEnvironment(jobExecutor, getParallelism(), getConfig().isObjectReuseEnabled());
				return lastEnv;
			}
		};

		initializeContextEnvironment(factory);
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Sets the current {@link ExecutionEnvironment} to be a {@link TestEnvironment}. The test
	 * environment executes the given jobs on a Flink mini cluster with the given default
	 * parallelism and the additional jar files and class paths.
	 *
	 * @param jobExecutor The executor to run the jobs on
	 * @param parallelism The default parallelism
	 * @param jarFiles Additional jar files to execute the job with
	 * @param classPaths Additional class paths to execute the job with
	 */
	public static void setAsContext(
		final JobExecutor jobExecutor,
		final int parallelism,
		final Collection<Path> jarFiles,
		final Collection<URL> classPaths) {

		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				return new TestEnvironment(
					jobExecutor,
					parallelism,
					false,
					jarFiles,
					classPaths
				);
			}
		};

		initializeContextEnvironment(factory);
	}

	/**
	 * Sets the current {@link ExecutionEnvironment} to be a {@link TestEnvironment}. The test
	 * environment executes the given jobs on a Flink mini cluster with the given default
	 * parallelism and the additional jar files and class paths.
	 *
	 * @param jobExecutor The executor to run the jobs on
	 * @param parallelism The default parallelism
	 */
	public static void setAsContext(final JobExecutor jobExecutor, final int parallelism) {
		setAsContext(
			jobExecutor,
			parallelism,
			Collections.emptyList(),
			Collections.emptyList());
	}

	public static void unsetAsContext() {
		resetContextEnvironment();
	}
}
