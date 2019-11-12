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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Component to retrieve the inputs locations of a {@link Execution}.
 */
public interface InputsLocationsRetriever {

	/**
	 * Get the producers of the result partitions consumed by an execution.
	 *
	 * @param executionVertexId identifies the execution
	 * @return the producers of the result partitions group by job vertex id
	 */
	Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(ExecutionVertexID executionVertexId);

	/**
	 * Get the task manager location future for an execution.
	 *
	 * @param executionVertexId identifying the execution
	 * @return the task manager location future
	 */
	Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(ExecutionVertexID executionVertexId);
}
