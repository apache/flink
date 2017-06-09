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
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Preconditions;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link StreamExecutionEnvironment} that executes its jobs on {@link LocalFlinkMiniCluster}.
 */
public class TestStreamEnvironment extends StreamExecutionEnvironment {

	/** The mini cluster in which this environment executes its jobs. */
	private final LocalFlinkMiniCluster miniCluster;

	private final Collection<Path> jarFiles;

	private final Collection<URL> classPaths;

	public TestStreamEnvironment(
			LocalFlinkMiniCluster miniCluster,
			int parallelism,
			Collection<Path> jarFiles,
			Collection<URL> classPaths) {

		this.miniCluster = Preconditions.checkNotNull(miniCluster);
		this.jarFiles = Preconditions.checkNotNull(jarFiles);
		this.classPaths = Preconditions.checkNotNull(classPaths);

		setParallelism(parallelism);
	}

	public TestStreamEnvironment(
			LocalFlinkMiniCluster miniCluster,
			int parallelism) {
		this(miniCluster, parallelism, Collections.<Path>emptyList(), Collections.<URL>emptyList());
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		final StreamGraph streamGraph = getStreamGraph();
		streamGraph.setJobName(jobName);
		final JobGraph jobGraph = streamGraph.getJobGraph();

		for (Path jarFile: jarFiles) {
			jobGraph.addJar(jarFile);
		}

		jobGraph.setClasspaths(new ArrayList<>(classPaths));

		return miniCluster.submitJobAndWait(jobGraph, false);
	}

	// ------------------------------------------------------------------------

	/**
	 * Sets the streaming context environment to a TestStreamEnvironment that runs its programs on
	 * the given cluster with the given default parallelism and the specified jar files and class
	 * paths.
	 *
	 * @param cluster The test cluster to run the test program on.
	 * @param parallelism The default parallelism for the test programs.
	 * @param jarFiles Additional jar files to execute the job with
	 * @param classpaths Additional class paths to execute the job with
	 */
	public static void setAsContext(
			final LocalFlinkMiniCluster cluster,
			final int parallelism,
			final Collection<Path> jarFiles,
			final Collection<URL> classpaths) {

		StreamExecutionEnvironmentFactory factory = new StreamExecutionEnvironmentFactory() {
			@Override
			public StreamExecutionEnvironment createExecutionEnvironment() {
				return new TestStreamEnvironment(
					cluster,
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
	 * @param cluster The test cluster to run the test program on.
	 * @param parallelism The default parallelism for the test programs.
	 */
	public static void setAsContext(final LocalFlinkMiniCluster cluster, final int parallelism) {
		setAsContext(
			cluster,
			parallelism,
			Collections.<Path>emptyList(),
			Collections.<URL>emptyList());
	}

	/**
	 * Resets the streaming context environment to null.
	 */
	public static void unsetAsContext() {
		resetContextEnvironment();
	}
}
