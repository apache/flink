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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.JobExecutor;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Preconditions;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link StreamExecutionEnvironment} that executes its jobs on {@link MiniCluster}.
 */
public class TestStreamEnvironment extends StreamExecutionEnvironment {

	/** The job executor to use to execute environment's jobs. */
	private final JobExecutor jobExecutor;

	private final Collection<Path> jarFiles;

	private final Collection<URL> classPaths;

	public TestStreamEnvironment(
			JobExecutor jobExecutor,
			int parallelism,
			Collection<Path> jarFiles,
			Collection<URL> classPaths) {

		this.jobExecutor = Preconditions.checkNotNull(jobExecutor);
		this.jarFiles = Preconditions.checkNotNull(jarFiles);
		this.classPaths = Preconditions.checkNotNull(classPaths);

		setParallelism(parallelism);
	}

	public TestStreamEnvironment(
			JobExecutor jobExecutor,
			int parallelism) {
		this(jobExecutor, parallelism, Collections.emptyList(), Collections.emptyList());
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		final JobGraph jobGraph = streamGraph.getJobGraph();

		for (Path jarFile : jarFiles) {
			jobGraph.addJar(jarFile);
		}

		jobGraph.setClasspaths(new ArrayList<>(classPaths));

		return jobExecutor.executeJobBlocking(jobGraph);
	}

	// ------------------------------------------------------------------------

	/**
	 * Sets the streaming context environment to a TestStreamEnvironment that runs its programs on
	 * the given cluster with the given default parallelism and the specified jar files and class
	 * paths.
	 *
	 * @param jobExecutor The executor to execute the jobs on
	 * @param parallelism The default parallelism for the test programs.
	 * @param jarFiles Additional jar files to execute the job with
	 * @param classpaths Additional class paths to execute the job with
	 */
	public static void setAsContext(
			final JobExecutor jobExecutor,
			final int parallelism,
			final Collection<Path> jarFiles,
			final Collection<URL> classpaths) {

		StreamExecutionEnvironmentFactory factory = new StreamExecutionEnvironmentFactory() {
			@Override
			public StreamExecutionEnvironment createExecutionEnvironment() {
				return new TestStreamEnvironment(
					jobExecutor,
					parallelism,
					jarFiles,
					classpaths);
			}
		};

		initializeContextEnvironment(factory);
	}

	/**
	 * Sets the streaming context environment to a TestStreamEnvironment that runs its programs on
	 * the given cluster with the given default parallelism.
	 *
	 * @param jobExecutor The executor to execute the jobs on
	 * @param parallelism The default parallelism for the test programs.
	 */
	public static void setAsContext(final JobExecutor jobExecutor, final int parallelism) {
		setAsContext(
			jobExecutor,
			parallelism,
			Collections.emptyList(),
			Collections.emptyList());
	}

	/**
	 * Resets the streaming context environment to null.
	 */
	public static void unsetAsContext() {
		resetContextEnvironment();
	}
}
