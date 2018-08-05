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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The LocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded
 * Flink cluster in the background and executes the program on that cluster.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 *
 * <p>Local environments can also be instantiated through {@link StreamExecutionEnvironment#createLocalEnvironment()}
 * and {@link StreamExecutionEnvironment#createLocalEnvironment(int)}. The former version will pick a
 * default parallelism equal to the number of hardware contexts in the local machine.
 */
@Public
public class LegacyLocalStreamEnvironment extends LocalStreamEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);

	public LegacyLocalStreamEnvironment() {
		this(new Configuration());
	}

	/**
	 * Creates a new local stream environment that configures its local executor with the given configuration.
	 *
	 * @param config The configuration used to configure the local executor.
	 */
	public LegacyLocalStreamEnvironment(Configuration config) {
		super(config);
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

		JobGraph jobGraph = streamGraph.getJobGraph();

		Configuration configuration = new Configuration();
		configuration.addAll(jobGraph.getJobConfiguration());

		configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

		// add (and override) the settings with what the user defined
		configuration.addAll(getConfiguration());

		if (LOG.isInfoEnabled()) {
			LOG.info("Running job on local embedded Flink mini cluster");
		}

		LocalFlinkMiniCluster exec = new LocalFlinkMiniCluster(configuration, true);
		try {
			exec.start();
			return exec.submitJobAndWait(jobGraph, getConfig().isSysoutLoggingEnabled());
		}
		finally {
			transformations.clear();
			exec.stop();
		}
	}
}
