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
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.util.Preconditions;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link ExecutionEnvironment} implementation which executes its jobs on a
 * {@link LocalFlinkMiniCluster}.
 */
public class TestEnvironment extends ExecutionEnvironment {

	private final LocalFlinkMiniCluster miniCluster;

	private final Collection<Path> jarFiles;

	private final Collection<URL> classPaths;

	private TestEnvironment lastEnv;

	public TestEnvironment(
			LocalFlinkMiniCluster miniCluster,
			int parallelism,
			boolean isObjectReuseEnabled,
			Collection<Path> jarFiles,
			Collection<URL> classPaths) {
		this.miniCluster = Preconditions.checkNotNull(miniCluster);
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
			LocalFlinkMiniCluster executor,
			int parallelism,
			boolean isObjectReuseEnabled) {
		this(
			executor,
			parallelism,
			isObjectReuseEnabled,
			Collections.<Path>emptyList(),
			Collections.<URL>emptyList());
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

		this.lastJobExecutionResult = miniCluster.submitJobAndWait(jobGraph, false);
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

		Optimizer pc = new Optimizer(new DataStatistics(), this.miniCluster.configuration());
		return pc.compile(p);
	}

	public void setAsContext() {
		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				lastEnv = new TestEnvironment(miniCluster, getParallelism(), getConfig().isObjectReuseEnabled());
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
	 * @param miniCluster The mini cluster on which to execute the jobs
	 * @param parallelism The default parallelism
	 * @param jarFiles Additional jar files to execute the job with
	 * @param classPaths Additional class paths to execute the job with
	 */
	public static void setAsContext(
		final LocalFlinkMiniCluster miniCluster,
		final int parallelism,
		final Collection<Path> jarFiles,
		final Collection<URL> classPaths) {

		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				return new TestEnvironment(
					miniCluster,
					parallelism,
					false, jarFiles,
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
	 * @param miniCluster The mini cluster on which to execute the jobs
	 * @param parallelism The default parallelism
	 */
	public static void setAsContext(final LocalFlinkMiniCluster miniCluster, final int parallelism) {
		setAsContext(
			miniCluster,
			parallelism,
			Collections.<Path>emptyList(),
			Collections.<URL>emptyList());
	}

	public static void unsetAsContext() {
		resetContextEnvironment();
	}
}
