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
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Synchronous version of {@link DefaultPreferredLocationsRetriever}.
 *
 * <p>This class turns {@link DefaultPreferredLocationsRetriever} into {@link SyncPreferredLocationsRetriever}.
 * The method {@link #getPreferredLocations(ExecutionVertexID, Set)} does not return {@link CompletableFuture}
 * of preferred locations, it returns only locations which are available immediately.
 * This behaviour is achieved by wrapping the original {@link InputsLocationsRetriever} with
 * {@link AvailableInputsLocationsRetriever} and hence making it synchronous without blocking.
 * As {@link StateLocationRetriever} is already synchronous,
 * the overall location retrieval becomes synchronous without blocking.
 */
class DefaultSyncPreferredLocationsRetriever implements SyncPreferredLocationsRetriever {
	private final PreferredLocationsRetriever asyncPreferredLocationsRetriever;

	DefaultSyncPreferredLocationsRetriever(
			StateLocationRetriever stateLocationRetriever,
			InputsLocationsRetriever inputsLocationsRetriever) {
		this.asyncPreferredLocationsRetriever = new DefaultPreferredLocationsRetriever(
				stateLocationRetriever,
				new AvailableInputsLocationsRetriever(inputsLocationsRetriever));
	}

	@Override
	public Collection<TaskManagerLocation> getPreferredLocations(
			ExecutionVertexID executionVertexId,
			Set<ExecutionVertexID> producersToIgnore) {
		CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture =
				asyncPreferredLocationsRetriever.getPreferredLocations(executionVertexId, producersToIgnore);
		Preconditions.checkState(preferredLocationsFuture.isDone());
		// it is safe to do the blocking call here
		// as the underlying InputsLocationsRetriever returns only immediately available locations
		return preferredLocationsFuture.join();
	}
}
