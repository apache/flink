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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

/** Tests for {@link DefaultSyncPreferredLocationsRetriever}. */
public class DefaultSyncPreferredLocationsRetrieverTest extends TestLogger {
    private static final ExecutionVertexID EV1 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV2 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV3 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV4 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV5 = createRandomExecutionVertexId();

    @Test
    public void testAvailableInputLocationRetrieval() {
        TestingInputsLocationsRetriever originalLocationRetriever =
                new TestingInputsLocationsRetriever.Builder()
                        .connectConsumerToProducer(EV5, EV1)
                        .connectConsumerToProducer(EV5, EV2)
                        .connectConsumerToProducer(EV5, EV3)
                        .connectConsumerToProducer(EV5, EV4)
                        .build();

        originalLocationRetriever.assignTaskManagerLocation(EV1);
        originalLocationRetriever.markScheduled(EV2);
        originalLocationRetriever.failTaskManagerLocation(EV3, new Throwable());
        originalLocationRetriever.cancelTaskManagerLocation(EV4);

        SyncPreferredLocationsRetriever locationsRetriever =
                new DefaultSyncPreferredLocationsRetriever(
                        executionVertexId -> Optional.empty(), originalLocationRetriever);

        Collection<TaskManagerLocation> preferredLocations =
                locationsRetriever.getPreferredLocations(EV5, Collections.emptySet());
        TaskManagerLocation expectedLocation =
                originalLocationRetriever.getTaskManagerLocation(EV1).get().join();

        assertThat(preferredLocations, contains(expectedLocation));
    }
}
