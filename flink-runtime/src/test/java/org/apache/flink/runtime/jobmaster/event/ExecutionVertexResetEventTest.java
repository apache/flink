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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ExecutionVertexResetEvent}. */
class ExecutionVertexResetEventTest {

    @Test
    void testSerializeExecutionVertexResetEvent() throws IOException {
        JobVertexID jobVertexId = new JobVertexID();
        ExecutionVertexID executionVertexId1 = new ExecutionVertexID(jobVertexId, 0);
        ExecutionVertexID executionVertexId2 = new ExecutionVertexID(jobVertexId, 1);
        ExecutionVertexID executionVertexId3 = new ExecutionVertexID(jobVertexId, 2);
        ExecutionVertexResetEvent event =
                new ExecutionVertexResetEvent(
                        Arrays.asList(executionVertexId1, executionVertexId2, executionVertexId3));

        final GenericJobEventSerializer serializer = new GenericJobEventSerializer();
        byte[] binary = serializer.serialize(event);
        ExecutionVertexResetEvent deserializeEvent =
                (ExecutionVertexResetEvent) serializer.deserialize(serializer.getVersion(), binary);

        assertThat(event.getExecutionVertexIds().equals(deserializeEvent.getExecutionVertexIds()))
                .isTrue();
    }
}
