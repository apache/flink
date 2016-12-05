/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Flip6LocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded
 * Flink cluster in the background and executes the program on that cluster.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 */
@Public
public class Flip6LocalStreamEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(Flip6LocalStreamEnvironment.class);

	/** The configuration to use for the mini cluster */
	private final Configuration conf;

	/**
	 * Creates a new mini cluster stream environment that uses the default configuration.
	 */
	public Flip6LocalStreamEnvironment() {
		this(null);
	}

	/**
	 * Creates a new mini cluster stream environment that configures its local executor with the given configuration.
	 *
	 * @param config The configuration used to configure the local executor.
	 */
	public Flip6LocalStreamEnvironment(Configuration config) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The Flip6LocalStreamEnvironment cannot be used when submitting a program through a client, " +
							"or running in a TestEnvironment context.");
		}

		this.conf = config == null ? new Configuration() : config;
		setParallelism(1);
	}

	/**
	 * Executes the JobGraph of the on a mini cluster of CLusterUtil with a user
	 * specified name.
	 * 
	 * @param jobName
	 *            name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		// transform the streaming program into a JobGraph
		StreamGraph streamGraph = getStreamGraph();
		streamGraph.setJobName(jobName);

		// TODO - temp fix to enforce restarts due to a bug in the allocation protocol
		streamGraph.getExecutionConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 5));

		JobGraph jobGraph = streamGraph.getJobGraph();
		jobGraph.setAllowQueuedScheduling(true);

		Configuration configuration = new Configuration();
		configuration.addAll(jobGraph.getJobConfiguration());
		configuration.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1L);

		// add (and override) the settings with what the user defined
		configuration.addAll(this.conf);

		MiniClusterConfiguration cfg = new MiniClusterConfiguration(configuration);

		// Currently we do not reuse slot anymore,
		// so we need to sum up the parallelism of all vertices
		int slotsCount = 0;
		for (JobVertex jobVertex : jobGraph.getVertices()) {
			slotsCount += jobVertex.getParallelism();
		}
		cfg.setNumTaskManagerSlots(slotsCount);

		if (LOG.isInfoEnabled()) {
			LOG.info("Running job on local embedded Flink mini cluster");
		}

		MiniCluster miniCluster = new MiniCluster(cfg);
		try {
			miniCluster.start();
			miniCluster.waitUntilTaskManagerRegistrationsComplete();
			return miniCluster.runJobBlocking(jobGraph);
		}
		finally {
			transformations.clear();
			miniCluster.shutdown();
		}
	}
}
