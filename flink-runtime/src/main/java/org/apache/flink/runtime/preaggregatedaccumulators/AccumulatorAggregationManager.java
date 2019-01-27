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

package org.apache.flink.runtime.preaggregatedaccumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * The manager for pre-aggregated accumulators in task executor side. It is responsible for partially
 * aggregating the pre-aggregated accumulators before sending them to the job master side, and querying
 * the pre-aggregated accumulators on behalf of tasks.
 */
public interface AccumulatorAggregationManager {

	/**
	 * Registers on a pre-aggregated accumulator for a task, which indicates that the task will
	 * commit this accumulator later.
	 *
	 * @param jobId JobID of the registering task.
	 * @param jobVertexId JobVertexID of the registering task.
	 * @param subtaskIndex subtaskIndex of the registering task.
	 * @param name The name of the accumulator to register.
	 */
	void registerPreAggregatedAccumulator(JobID jobId, JobVertexID jobVertexId, int subtaskIndex, String name);

	/**
	 * Commits the final value of a task for the specific accumulator. The committed task
	 * should have already registered on the accumulator and have not committed before.
	 *
	 * @param jobId JobID of the committing task.
	 * @param jobVertexId JobVertexID of the committing task.
	 * @param subtaskIndex subtaskIndex of the committing task.
	 * @param name The name of the accumulator to commit.
	 * @param value The committing pre-aggregated accumulator's value.
	 */
	void commitPreAggregatedAccumulator(JobID jobId, JobVertexID jobVertexId, int subtaskIndex, String name, Accumulator value);

	/**
	 * Queries the aggregated value of a specific pre-aggregated accumulator asynchronously.
	 *
	 * @param jobId JobID of the querying task.
	 * @param name The name of the filter to  on.
	 * @return The Future object for the querying pre-aggregated accumulator.
	 */
	<V, A extends Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(JobID jobId, String name);

	/**
	 * Clears the registration status of a task if it has not committed yet when the task exits.
	 *
	 * @param jobId JobID of the exited task.
	 * @param jobVertexId JobVertexID of the exited task.
	 * @param subtaskIndex subtaskIndex of the exiting task.
	 */
	void clearRegistrationForTask(JobID jobId, JobVertexID jobVertexId, int subtaskIndex);

	/**
	 * Removes all the aggregating and querying accumulators for the specific job when the connection to the
	 * JobMaster is closed.
	 *
	 * @param jobId JobID of the job to clear status.
	 */
	void clearAccumulatorsForJob(JobID jobId);
}
