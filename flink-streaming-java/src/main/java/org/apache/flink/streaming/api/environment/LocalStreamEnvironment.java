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
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraph;

import javax.annotation.Nonnull;

import java.util.Collections;

/**
 * The LocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded
 * Flink cluster in the background and executes the program on that cluster.
 *
 * <p>When this environment is instantiated, it uses a default parallelism of {@code 1}. The default
 * parallelism can be set via {@link #setParallelism(int)}.
 */
@Public
public class LocalStreamEnvironment extends StreamExecutionEnvironment {

	private final Configuration configuration;

	/**
	 * Creates a new mini cluster stream environment that uses the default configuration.
	 */
	public LocalStreamEnvironment() {
		this(new Configuration());
	}

	/**
	 * Creates a new mini cluster stream environment that configures its local executor with the given configuration.
	 *
	 * @param configuration The configuration used to configure the local executor.
	 */
	public LocalStreamEnvironment(@Nonnull Configuration configuration) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
				"The LocalStreamEnvironment cannot be used when submitting a program through a client, " +
					"or running in a TestEnvironment context.");
		}
		this.configuration = configuration;
		setParallelism(1);
	}

	protected Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Executes the JobGraph of the on a mini cluster of ClusterUtil with a user
	 * specified name.
	 *
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		try {
			final PlanExecutor executor = PlanExecutor.createLocalExecutor(configuration);
			return executor.executePlan(streamGraph, Collections.emptyList(), Collections.emptyList());
		} finally {
			transformations.clear();
		}
	}
}
