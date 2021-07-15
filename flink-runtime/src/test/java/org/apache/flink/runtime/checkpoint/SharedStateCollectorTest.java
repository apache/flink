/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.DefaultSubtaskAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.state.StateObjectID;

import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.flink.runtime.checkpoint.StateObjectIdCollectingVisitorTest.taskStateSnapshot;
import static org.junit.Assert.assertEquals;

/** {@link SharedStateCollector} test. */
public class SharedStateCollectorTest {

    @Test
    public void testNoSharing() throws Exception {
        ExecutionJobVertex vertex = vertex(5);
        for (int i = 0; i < vertex.getParallelism(); i++) {
            setSnapshot(i, vertex, Integer.toString(i));
        }
        assertSharedStateEquals(emptySet(), vertex);
    }

    @Test
    public void testSimpleSharing() throws Exception {
        ExecutionJobVertex vertex = vertex(2);
        String stateId = "shared";
        setSnapshot(0, vertex, stateId);
        setSnapshot(1, vertex, stateId);
        assertSharedStateEquals(singleton(stateId), vertex);
    }

    @Test
    public void testCrossVertexSharing() throws Exception {
        ExecutionJobVertex vertex1 = vertex(1);
        ExecutionJobVertex vertex2 = vertex(1);
        String stateId = "shared";
        setSnapshot(0, vertex1, stateId);
        setSnapshot(0, vertex2, stateId);
        assertSharedStateEquals(singleton(stateId), vertex1, vertex2);
    }

    @Test
    public void testSameVertexIsNotSharing() throws Exception {
        ExecutionJobVertex vertex = vertex(1);
        setSnapshot(0, vertex, "abc", "abc");
        assertSharedStateEquals(emptySet(), vertex);
    }

    private void assertSharedStateEquals(Set<String> expected, ExecutionJobVertex... vertex) {
        assertEquals(
                expected.stream().map(StateObjectID::of).collect(Collectors.toSet()),
                SharedStateCollector.collect(asList(vertex)));
    }

    private ExecutionJobVertex vertex(int parallelism) throws JobException, JobExecutionException {
        return new ExecutionJobVertex(
                TestingDefaultExecutionGraphBuilder.newBuilder().build(),
                new JobVertex("testVertex"),
                0,
                Time.seconds(1),
                0,
                new DefaultVertexParallelismInfo(
                        parallelism, parallelism, ign0 -> Optional.empty()),
                new DefaultSubtaskAttemptNumberStore(Collections.emptyList()));
    }

    private void setSnapshot(int idx, ExecutionJobVertex vertex, String... states) {
        vertex.getTaskVertices()[idx]
                .getCurrentExecutionAttempt()
                .setInitialState(new JobManagerTaskRestore(0, taskStateSnapshot(states)));
    }
}
