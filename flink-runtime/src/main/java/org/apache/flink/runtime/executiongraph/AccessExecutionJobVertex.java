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
package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Common interface for the runtime {@link ExecutionJobVertex} and {@link ArchivedExecutionJobVertex}.
 */
public interface AccessExecutionJobVertex {
	/**
	 * Returns the name for this job vertex.
	 *
	 * @return name for this job vertex.
	 */
	String getName();

	/**
	 * Returns the parallelism for this job vertex.
	 *
	 * @return parallelism for this job vertex.
	 */
	int getParallelism();

	/**
	 * Returns the max parallelism for this job vertex.
	 *
	 * @return max parallelism for this job vertex.
	 */
	int getMaxParallelism();

	/**
	 * Returns the {@link JobVertexID} for this job vertex.
	 *
	 * @return JobVertexID for this job vertex.
	 */
	JobVertexID getJobVertexId();

	/**
	 * Returns all execution vertices for this job vertex.
	 *
	 * @return all execution vertices for this job vertex
	 */
	AccessExecutionVertex[] getTaskVertices();

	/**
	 * Returns the aggregated {@link ExecutionState} for this job vertex.
	 *
	 * @return aggregated state for this job vertex
	 */
	ExecutionState getAggregateState();

	/**
	 * Returns the aggregated user-defined accumulators as strings.
	 *
	 * @return aggregated user-defined accumulators as strings.
	 */
	StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified();

}
