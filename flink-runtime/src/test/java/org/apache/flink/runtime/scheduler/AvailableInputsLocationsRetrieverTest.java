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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link AvailableInputsLocationsRetriever}. */
public class AvailableInputsLocationsRetrieverTest extends TestLogger {
    private static final ExecutionVertexID EV1 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV2 = createRandomExecutionVertexId();

    @Test
    public void testNoInputLocation() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        assertThat(
                availableInputsLocationsRetriever.getTaskManagerLocation(EV1).isPresent(),
                is(false));
    }

    @Test
    public void testNoInputLocationIfNotDone() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        originalLocationRetriever.markScheduled(EV1);
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        assertThat(
                availableInputsLocationsRetriever.getTaskManagerLocation(EV1).isPresent(),
                is(false));
    }

    @Test
    public void testNoInputLocationIfFailed() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        originalLocationRetriever.failTaskManagerLocation(EV1, new Throwable());
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        assertThat(
                availableInputsLocationsRetriever.getTaskManagerLocation(EV1).isPresent(),
                is(false));
    }

    @Test
    public void testInputLocationIfDone() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        originalLocationRetriever.assignTaskManagerLocation(EV1);
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        assertThat(
                availableInputsLocationsRetriever.getTaskManagerLocation(EV1).isPresent(),
                is(true));
    }

    @Test
    public void testConsumedResultPartitionsProducers() {
        TestingInputsLocationsRetriever originalLocationRetriever = getOriginalLocationRetriever();
        InputsLocationsRetriever availableInputsLocationsRetriever =
                new AvailableInputsLocationsRetriever(originalLocationRetriever);
        Collection<Collection<ExecutionVertexID>> producers =
                availableInputsLocationsRetriever.getConsumedResultPartitionsProducers(EV2);
        assertThat(producers.size(), is(1));
        Collection<ExecutionVertexID> resultProducers = producers.iterator().next();
        assertThat(resultProducers, contains(EV1));
    }

    private static TestingInputsLocationsRetriever getOriginalLocationRetriever() {
        return new TestingInputsLocationsRetriever.Builder()
                .connectConsumerToProducer(EV2, EV1)
                .build();
    }
}
