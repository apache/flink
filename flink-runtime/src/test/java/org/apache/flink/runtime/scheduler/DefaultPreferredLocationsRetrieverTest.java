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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.scheduler.DefaultPreferredLocationsRetriever.MAX_DISTINCT_CONSUMERS_TO_CONSIDER;
import static org.apache.flink.runtime.scheduler.DefaultPreferredLocationsRetriever.MAX_DISTINCT_LOCATIONS_TO_CONSIDER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link DefaultPreferredLocationsRetriever}. */
class DefaultPreferredLocationsRetrieverTest {

    @Test
    void testStateLocationsWillBeReturnedIfExist() {
        final TaskManagerLocation stateLocation = new LocalTaskManagerLocation();

        final TestingInputsLocationsRetriever.Builder locationRetrieverBuilder =
                new TestingInputsLocationsRetriever.Builder();

        final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);
        final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);
        locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId);

        final TestingInputsLocationsRetriever inputsLocationsRetriever =
                locationRetrieverBuilder.build();

        inputsLocationsRetriever.markScheduled(producerId);

        final PreferredLocationsRetriever locationsRetriever =
                new DefaultPreferredLocationsRetriever(
                        id -> Optional.of(stateLocation), inputsLocationsRetriever);

        final CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
                locationsRetriever.getPreferredLocations(consumerId, Collections.emptySet());

        assertThat(preferredLocations.getNow(null)).containsExactly(stateLocation);
    }

    @Test
    void testInputLocations() {
        final List<TaskManagerLocation> producerLocations =
                Collections.singletonList(new LocalTaskManagerLocation());
        testInputLocationsInternal(
                1,
                MAX_DISTINCT_CONSUMERS_TO_CONSIDER,
                producerLocations,
                producerLocations,
                Collections.emptySet());
    }

    @Test
    void testInputLocationsIgnoresEdgeOfTooManyProducers() {
        testNoPreferredInputLocationsInternal(MAX_DISTINCT_LOCATIONS_TO_CONSIDER + 1, 1);
    }

    @Test
    void testInputLocationsIgnoresEdgeOfTooManyConsumers() {
        testNoPreferredInputLocationsInternal(1, MAX_DISTINCT_CONSUMERS_TO_CONSIDER + 1);
        testNoPreferredInputLocationsInternal(2, MAX_DISTINCT_CONSUMERS_TO_CONSIDER + 1);
    }

    @Test
    void testInputLocationsChoosesInputOfFewerLocations() {
        final TestingInputsLocationsRetriever.Builder locationRetrieverBuilder =
                new TestingInputsLocationsRetriever.Builder();

        final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

        int parallelism1 = 3;
        final JobVertexID jobVertexId1 = new JobVertexID();
        final List<ExecutionVertexID> producers1 = new ArrayList<>(parallelism1);
        for (int i = 0; i < parallelism1; i++) {
            final ExecutionVertexID producerId = new ExecutionVertexID(jobVertexId1, i);
            producers1.add(producerId);
        }
        locationRetrieverBuilder.connectConsumerToProducers(consumerId, producers1);

        final JobVertexID jobVertexId2 = new JobVertexID();
        int parallelism2 = 5;
        final List<ExecutionVertexID> producers2 = new ArrayList<>(parallelism2);
        for (int i = 0; i < parallelism2; i++) {
            final ExecutionVertexID producerId = new ExecutionVertexID(jobVertexId2, i);
            producers2.add(producerId);
        }
        locationRetrieverBuilder.connectConsumerToProducers(consumerId, producers2);

        final TestingInputsLocationsRetriever inputsLocationsRetriever =
                locationRetrieverBuilder.build();

        final List<TaskManagerLocation> expectedLocations = new ArrayList<>(parallelism1);
        for (int i = 0; i < parallelism1; i++) {
            inputsLocationsRetriever.assignTaskManagerLocation(producers1.get(i));
            expectedLocations.add(
                    inputsLocationsRetriever
                            .getTaskManagerLocation(producers1.get(i))
                            .get()
                            .getNow(null));
        }

        for (int i = 0; i < parallelism2; i++) {
            inputsLocationsRetriever.assignTaskManagerLocation(producers2.get(i));
        }

        final PreferredLocationsRetriever locationsRetriever =
                new DefaultPreferredLocationsRetriever(
                        id -> Optional.empty(), inputsLocationsRetriever);

        final CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
                locationsRetriever.getPreferredLocations(consumerId, Collections.emptySet());

        assertThat(preferredLocations.getNow(null))
                .containsExactlyInAnyOrderElementsOf(expectedLocations);
    }

    @Test
    void testInputLocationsIgnoresExcludedProducers() {
        final List<TaskManagerLocation> producerLocations =
                Arrays.asList(new LocalTaskManagerLocation(), new LocalTaskManagerLocation());
        final Set<Integer> producersToIgnore = Collections.singleton(0);
        testInputLocationsInternal(
                2, 1, producerLocations, producerLocations.subList(1, 2), producersToIgnore);
    }

    private void testNoPreferredInputLocationsInternal(
            final int producerParallelism, final int consumerParallelism) {
        testInputLocationsInternal(
                producerParallelism,
                consumerParallelism,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptySet());
    }

    private void testInputLocationsInternal(
            final int producerParallelism,
            final int consumerParallelism,
            final List<TaskManagerLocation> producerLocations,
            final List<TaskManagerLocation> expectedPreferredLocations,
            final Set<Integer> indicesOfProducersToIgnore) {

        final JobVertexID producerJobVertexId = new JobVertexID();
        final List<ExecutionVertexID> producerIds =
                IntStream.range(0, producerParallelism)
                        .mapToObj(i -> new ExecutionVertexID(producerJobVertexId, i))
                        .collect(Collectors.toList());

        final JobVertexID consumerJobVertexId = new JobVertexID();
        final List<ExecutionVertexID> consumerIds =
                IntStream.range(0, consumerParallelism)
                        .mapToObj(i -> new ExecutionVertexID(consumerJobVertexId, i))
                        .collect(Collectors.toList());

        final TestingInputsLocationsRetriever.Builder locationRetrieverBuilder =
                new TestingInputsLocationsRetriever.Builder();
        locationRetrieverBuilder.connectConsumersToProducers(consumerIds, producerIds);

        final TestingInputsLocationsRetriever inputsLocationsRetriever =
                locationRetrieverBuilder.build();
        for (int i = 0; i < producerParallelism; i++) {
            TaskManagerLocation producerLocation;
            if (producerLocations.isEmpty()) {
                // generate a random location if not specified
                producerLocation = new LocalTaskManagerLocation();
            } else {
                producerLocation = producerLocations.get(i);
            }
            inputsLocationsRetriever.assignTaskManagerLocation(
                    producerIds.get(i), producerLocation);
        }

        checkInputLocations(
                consumerIds.get(0),
                inputsLocationsRetriever,
                expectedPreferredLocations,
                indicesOfProducersToIgnore.stream()
                        .map(index -> new ExecutionVertexID(producerJobVertexId, index))
                        .collect(Collectors.toSet()));
    }

    private void checkInputLocations(
            final ExecutionVertexID consumerId,
            final TestingInputsLocationsRetriever inputsLocationsRetriever,
            final List<TaskManagerLocation> expectedPreferredLocations,
            final Set<ExecutionVertexID> producersToIgnore) {

        final PreferredLocationsRetriever locationsRetriever =
                new DefaultPreferredLocationsRetriever(
                        id -> Optional.empty(), inputsLocationsRetriever);

        final CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
                locationsRetriever.getPreferredLocations(consumerId, producersToIgnore);

        assertThat(preferredLocations.getNow(null))
                .containsExactlyInAnyOrderElementsOf(expectedPreferredLocations);
    }
}
