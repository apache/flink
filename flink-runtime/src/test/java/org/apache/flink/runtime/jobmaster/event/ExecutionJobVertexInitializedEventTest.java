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

import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ExecutionJobVertexInitializedEvent}. */
class ExecutionJobVertexInitializedEventTest {

    @Test
    void testSerializeExecutionJobVertexInitializedEvent() throws IOException {
        int parallelism = 4;
        Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos = new HashMap<>();
        jobVertexInputInfos.put(
                new IntermediateDataSetID(), new JobVertexInputInfo(Collections.emptyList()));
        jobVertexInputInfos.put(
                new IntermediateDataSetID(),
                new JobVertexInputInfo(
                        Collections.singletonList(
                                new ExecutionVertexInputInfo(
                                        0, new IndexRange(0, 1), new IndexRange(0, 1)))));
        JobVertexID jobVertexId = new JobVertexID();

        ExecutionJobVertexInitializedEvent event =
                new ExecutionJobVertexInitializedEvent(
                        jobVertexId, parallelism, jobVertexInputInfos);

        final GenericJobEventSerializer serializer = new GenericJobEventSerializer();
        byte[] binary = serializer.serialize(event);
        ExecutionJobVertexInitializedEvent deserializeEvent =
                (ExecutionJobVertexInitializedEvent)
                        serializer.deserialize(serializer.getVersion(), binary);

        assertThat(deserializeEvent.getJobVertexId()).isEqualTo(jobVertexId);
        assertThat(deserializeEvent.getParallelism()).isEqualTo(parallelism);
        assertThat(deserializeEvent.getJobVertexInputInfos()).isEqualTo(jobVertexInputInfos);
        assertThat(deserializeEvent.getType()).isEqualTo(event.getType());
    }
}
