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

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvailableInputsLocationsRetriever}. */
class AvailableInputsLocationsRetrieverTest {
    private static final ExecutionVertexID EV1 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV2 = createRandomExecutionVertexId();

    @Test
    void testNoInputLocation() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        assertThat(availableInputsLocationsRetriever.getTaskManagerLocation(EV1)).isNotPresent();
    }

    @Test
    void testNoInputLocationIfNotDone() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        originalLocationRetriever.markScheduled(EV1);
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        assertThat(availableInputsLocationsRetriever.getTaskManagerLocation(EV1)).isNotPresent();
    }

    @Test
    void testNoInputLocationIfFailed() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        originalLocationRetriever.failTaskManagerLocation(EV1, new Throwable());
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        assertThat(availableInputsLocationsRetriever.getTaskManagerLocation(EV1)).isNotPresent();
    }

    @Test
    void testInputLocationIfDone() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        originalLocationRetriever.assignTaskManagerLocation(EV1);
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        assertThat(availableInputsLocationsRetriever.getTaskManagerLocation(EV1)).isPresent();
    }

    @Test
    void testGetConsumedPartitionGroupAndProducers() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);

        ConsumedPartitionGroup consumedPartitionGroup =
                Iterables.getOnlyElement(
                        (availableInputsLocationsRetriever.getConsumedPartitionGroups(EV2)));
        assertThat(consumedPartitionGroup).hasSize(1);

        Collection<ExecutionVertexID> producers =
                availableInputsLocationsRetriever.getProducersOfConsumedPartitionGroup(
                        consumedPartitionGroup);
        assertThat(producers).containsExactly(EV1);
    }

    private static TestingInputsLocationsRetriever getOriginalLocationRetriever() {
        return new TestingInputsLocationsRetriever.Builder()
                .connectConsumerToProducer(EV2, EV1)
                .build();
    }
}
