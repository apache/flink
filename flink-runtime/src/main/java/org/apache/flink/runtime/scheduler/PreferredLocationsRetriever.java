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

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Component to retrieve the preferred locations of an execution vertex.
 */
@FunctionalInterface
public interface PreferredLocationsRetriever {

	/**
	 * Returns preferred locations of an execution vertex.
	 *
	 * @param executionVertexId id of the execution vertex
	 * @param producersToIgnore producer vertices to ignore when calculating input locations
	 * @return future of preferred locations
	 */
	CompletableFuture<Collection<TaskManagerLocation>> getPreferredLocations(
		ExecutionVertexID executionVertexId,
		Set<ExecutionVertexID> producersToIgnore);
}
