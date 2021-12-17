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

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/** Tests {@link DefaultPreferredLocationsRetriever}. */
public class DefaultPreferredLocationsRetrieverTest extends TestLogger {

    @Test
    public void testStateLocationsWillBeReturnedIfExist() {
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

        assertThat(preferredLocations.getNow(null), contains(stateLocation));
    }

    @Test
    public void testInputLocationsIgnoresEdgeOfTooManyLocations() {
        final TestingInputsLocationsRetriever.Builder locationRetrieverBuilder =
                new TestingInputsLocationsRetriever.Builder();

        final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

        final int producerParallelism = ExecutionVertex.MAX_DISTINCT_LOCATIONS_TO_CONSIDER + 1;
        final List<ExecutionVertexID> producerIds = new ArrayList<>(producerParallelism);
        final JobVertexID producerJobVertexId = new JobVertexID();
        for (int i = 0; i < producerParallelism; i++) {
            final ExecutionVertexID producerId = new ExecutionVertexID(producerJobVertexId, i);
            locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId);
            producerIds.add(producerId);
        }

        final TestingInputsLocationsRetriever inputsLocationsRetriever =
                locationRetrieverBuilder.build();

        for (int i = 0; i < producerParallelism; i++) {
            inputsLocationsRetriever.markScheduled(producerIds.get(i));
        }

        final PreferredLocationsRetriever locationsRetriever =
                new DefaultPreferredLocationsRetriever(
                        id -> Optional.empty(), inputsLocationsRetriever);

        final CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
                locationsRetriever.getPreferredLocations(consumerId, Collections.emptySet());

        assertThat(preferredLocations.getNow(null), hasSize(0));
    }

    @Test
    public void testInputLocationsChoosesInputOfFewerLocations() {
        final TestingInputsLocationsRetriever.Builder locationRetrieverBuilder =
                new TestingInputsLocationsRetriever.Builder();

        final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

        int parallelism1 = 3;
        final JobVertexID jobVertexId1 = new JobVertexID();
        final List<ExecutionVertexID> producers1 = new ArrayList<>(parallelism1);
        for (int i = 0; i < parallelism1; i++) {
            final ExecutionVertexID producerId = new ExecutionVertexID(jobVertexId1, i);
            producers1.add(producerId);
            locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId);
        }

        final JobVertexID jobVertexId2 = new JobVertexID();
        int parallelism2 = 5;
        final List<ExecutionVertexID> producers2 = new ArrayList<>(parallelism2);
        for (int i = 0; i < parallelism2; i++) {
            final ExecutionVertexID producerId = new ExecutionVertexID(jobVertexId2, i);
            producers2.add(producerId);
            locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId);
        }

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

        assertThat(
                preferredLocations.getNow(null), containsInAnyOrder(expectedLocations.toArray()));
    }

    @Test
    public void testInputLocationsIgnoresExcludedProducers() {
        final TestingInputsLocationsRetriever.Builder locationRetrieverBuilder =
                new TestingInputsLocationsRetriever.Builder();

        final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

        final JobVertexID producerJobVertexId = new JobVertexID();

        final ExecutionVertexID producerId1 = new ExecutionVertexID(producerJobVertexId, 0);
        locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId1);

        final ExecutionVertexID producerId2 = new ExecutionVertexID(producerJobVertexId, 1);
        locationRetrieverBuilder.connectConsumerToProducer(consumerId, producerId2);

        final TestingInputsLocationsRetriever inputsLocationsRetriever =
                locationRetrieverBuilder.build();

        inputsLocationsRetriever.markScheduled(producerId1);
        inputsLocationsRetriever.markScheduled(producerId2);

        inputsLocationsRetriever.assignTaskManagerLocation(producerId1);
        inputsLocationsRetriever.assignTaskManagerLocation(producerId2);

        final PreferredLocationsRetriever locationsRetriever =
                new DefaultPreferredLocationsRetriever(
                        id -> Optional.empty(), inputsLocationsRetriever);

        final CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
                locationsRetriever.getPreferredLocations(
                        consumerId, Collections.singleton(producerId1));

        assertThat(preferredLocations.getNow(null), hasSize(1));

        final TaskManagerLocation producerLocation2 =
                inputsLocationsRetriever.getTaskManagerLocation(producerId2).get().getNow(null);
        assertThat(preferredLocations.getNow(null), contains(producerLocation2));
    }
}
