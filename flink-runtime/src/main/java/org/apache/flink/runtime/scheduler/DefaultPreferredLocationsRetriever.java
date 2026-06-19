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

import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link PreferredLocationsRetriever}. Locations based on state will be
 * returned if exist. Otherwise locations based on inputs will be returned.
 */
public class DefaultPreferredLocationsRetriever implements PreferredLocationsRetriever {

    static final int MAX_DISTINCT_LOCATIONS_TO_CONSIDER = 8;

    static final int MAX_DISTINCT_CONSUMERS_TO_CONSIDER = 8;

    private final StateLocationRetriever stateLocationRetriever;

    private final InputsLocationsRetriever inputsLocationsRetriever;

    DefaultPreferredLocationsRetriever(
            final StateLocationRetriever stateLocationRetriever,
            final InputsLocationsRetriever inputsLocationsRetriever) {

        this.stateLocationRetriever = checkNotNull(stateLocationRetriever);
        this.inputsLocationsRetriever = checkNotNull(inputsLocationsRetriever);
    }

    @Override
    public CompletableFuture<Collection<TaskManagerLocation>> getPreferredLocations(
            final ExecutionVertexID executionVertexId,
            final Set<ExecutionVertexID> producersToIgnore) {

        checkNotNull(executionVertexId);
        checkNotNull(producersToIgnore);

        final Collection<TaskManagerLocation> preferredLocationsBasedOnState =
                getPreferredLocationsBasedOnState(executionVertexId);
        if (!preferredLocationsBasedOnState.isEmpty()) {
            return CompletableFuture.completedFuture(preferredLocationsBasedOnState);
        }

        return getPreferredLocationsBasedOnInputs(executionVertexId, producersToIgnore);
    }

    private Collection<TaskManagerLocation> getPreferredLocationsBasedOnState(
            final ExecutionVertexID executionVertexId) {

        return stateLocationRetriever
                .getStateLocation(executionVertexId)
                .map(Collections::singleton)
                .orElse(Collections.emptySet());
    }

    private CompletableFuture<Collection<TaskManagerLocation>> getPreferredLocationsBasedOnInputs(
            final ExecutionVertexID executionVertexId,
            final Set<ExecutionVertexID> producersToIgnore) {

        CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
                CompletableFuture.completedFuture(Collections.emptyList());

        final Collection<ConsumedPartitionGroup> consumedPartitionGroups =
                inputsLocationsRetriever.getConsumedPartitionGroups(executionVertexId);
        for (ConsumedPartitionGroup consumedPartitionGroup : consumedPartitionGroups) {
            // Ignore the location of a consumed partition group if it has too many distinct
            // consumers. This is to avoid tasks unevenly distributed on nodes when running batch
            // jobs or running jobs in session/standalone mode.
            if (consumedPartitionGroup.getConsumerVertexGroup().size()
                    > MAX_DISTINCT_CONSUMERS_TO_CONSIDER) {
                continue;
            }

            final Collection<CompletableFuture<TaskManagerLocation>> locationsFutures =
                    getInputLocationFutures(
                            producersToIgnore,
                            inputsLocationsRetriever.getProducersOfConsumedPartitionGroup(
                                    consumedPartitionGroup));

            preferredLocations = combineLocations(preferredLocations, locationsFutures);
        }
        return preferredLocations;
    }

    private Collection<CompletableFuture<TaskManagerLocation>> getInputLocationFutures(
            final Set<ExecutionVertexID> producersToIgnore,
            final Collection<ExecutionVertexID> producers) {

        final Collection<CompletableFuture<TaskManagerLocation>> locationsFutures =
                new ArrayList<>();

        for (ExecutionVertexID producer : producers) {
            final Optional<CompletableFuture<TaskManagerLocation>> optionalLocationFuture;
            if (!producersToIgnore.contains(producer)) {
                optionalLocationFuture = inputsLocationsRetriever.getTaskManagerLocation(producer);
            } else {
                optionalLocationFuture = Optional.empty();
            }
            optionalLocationFuture.ifPresent(locationsFutures::add);

            // inputs which have too many distinct sources are not considered because
            // input locality does not make much difference in this case and it could
            // be a long time to wait for all the location futures to complete
            if (locationsFutures.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
                return Collections.emptyList();
            }
        }

        return locationsFutures;
    }

    private CompletableFuture<Collection<TaskManagerLocation>> combineLocations(
            final CompletableFuture<Collection<TaskManagerLocation>> locationsCombinedAlready,
            final Collection<CompletableFuture<TaskManagerLocation>> locationsToCombine) {

        final CompletableFuture<Set<TaskManagerLocation>> uniqueLocationsFuture =
                FutureUtils.combineAll(locationsToCombine).thenApply(HashSet::new);

        return locationsCombinedAlready.thenCombine(
                uniqueLocationsFuture,
                (locationsOnOneEdge, locationsOnAnotherEdge) -> {
                    if ((!locationsOnOneEdge.isEmpty()
                                    && locationsOnAnotherEdge.size() > locationsOnOneEdge.size())
                            || locationsOnAnotherEdge.isEmpty()) {
                        return locationsOnOneEdge;
                    } else {
                        return locationsOnAnotherEdge;
                    }
                });
    }
}
