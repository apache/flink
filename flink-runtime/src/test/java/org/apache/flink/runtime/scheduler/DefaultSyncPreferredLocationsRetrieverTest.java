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
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultSyncPreferredLocationsRetriever}. */
class DefaultSyncPreferredLocationsRetrieverTest {
    private static final JobVertexID JV1 = new JobVertexID();
    private static final ExecutionVertexID EV11 = new ExecutionVertexID(JV1, 0);
    private static final ExecutionVertexID EV12 = new ExecutionVertexID(JV1, 1);
    private static final ExecutionVertexID EV13 = new ExecutionVertexID(JV1, 2);
    private static final ExecutionVertexID EV14 = new ExecutionVertexID(JV1, 3);
    private static final ExecutionVertexID EV21 = new ExecutionVertexID(new JobVertexID(), 0);

    @Test
    void testAvailableInputLocationRetrieval() {
        TestingInputsLocationsRetriever originalLocationRetriever =
                new TestingInputsLocationsRetriever.Builder()
                        .connectConsumerToProducers(EV21, Arrays.asList(EV11, EV12, EV13, EV14))
                        .build();

        originalLocationRetriever.assignTaskManagerLocation(EV11);
        originalLocationRetriever.markScheduled(EV12);
        originalLocationRetriever.failTaskManagerLocation(EV13, new Throwable());
        originalLocationRetriever.cancelTaskManagerLocation(EV14);

        SyncPreferredLocationsRetriever locationsRetriever =
                new DefaultSyncPreferredLocationsRetriever(
                        executionVertexId -> Optional.empty(), originalLocationRetriever);

        Collection<TaskManagerLocation> preferredLocations =
                locationsRetriever.getPreferredLocations(EV21, Collections.emptySet());
        TaskManagerLocation expectedLocation =
                originalLocationRetriever.getTaskManagerLocation(EV11).get().join();

        assertThat(preferredLocations).containsExactly(expectedLocation);
    }
}
