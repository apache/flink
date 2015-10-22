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

package org.apache.flink.storm.api;

import backtype.storm.generated.StormTopology;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * {@link FlinkTopology} mimics a {@link StormTopology} and is implemented in terms of a {@link
 * StreamExecutionEnvironment} . In contrast to a regular {@link StreamExecutionEnvironment}, a {@link FlinkTopology}
 * cannot be executed directly, but must be handed over to a {@link FlinkLocalCluster}, {@link FlinkSubmitter}, or
 * {@link FlinkClient}.
 */
public class FlinkTopology extends StreamExecutionEnvironment {

	/** The number of declared tasks for the whole program (ie, sum over all dops) */
	private int numberOfTasks = 0;

	public FlinkTopology() {
		// Set default parallelism to 1, to mirror Storm default behavior
		super.setParallelism(1);
	}

	/**
	 * Is not supported. In order to execute use {@link FlinkLocalCluster}, {@link FlinkSubmitter}, or {@link
	 * FlinkClient}.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public JobExecutionResult execute() throws Exception {
		throw new UnsupportedOperationException(
				"A FlinkTopology cannot be executed directly. Use FlinkLocalCluster, FlinkSubmitter, or FlinkClient " +
				"instead.");
	}

	/**
	 * Is not supported. In order to execute use {@link FlinkLocalCluster}, {@link FlinkSubmitter} or {@link
	 * FlinkClient}.
	 *
	 * @throws UnsupportedOperationException
	 * 		at every invocation
	 */
	@Override
	public JobExecutionResult execute(final String jobName) throws Exception {
		throw new UnsupportedOperationException(
				"A FlinkTopology cannot be executed directly. Use FlinkLocalCluster, FlinkSubmitter, or FlinkClient " +
				"instead.");
	}

	/**
	 * Increased the number of declared tasks of this program by the given value.
	 *
	 * @param dop
	 * 		The dop of a new operator that increases the number of overall tasks.
	 */
	public void increaseNumberOfTasks(final int dop) {
		assert (dop > 0);
		this.numberOfTasks += dop;
	}

	/**
	 * Return the number or required tasks to execute this program.
	 *
	 * @return the number or required tasks to execute this program
	 */
	public int getNumberOfTasks() {
		return this.numberOfTasks;
	}

}
