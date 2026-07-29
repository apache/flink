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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link NonChainedOutput}, focused on the adaptive-execution patching contract of
 * {@link NonChainedOutput#setTargetVertexId(JobVertexID)}.
 */
class NonChainedOutputTest {

    @Test
    void testTargetVertexIdCanBePatchedAfterCreation() {
        NonChainedOutput output = newOutput(/* targetVertexId */ null);

        assertThat(output.getTargetNodeId())
                .as("new NonChainedOutput in adaptive mode starts with a null target id")
                .isNull();

        JobVertexID downstreamId = new JobVertexID();
        output.setTargetVertexId(downstreamId);

        assertThat(output.getTargetNodeId())
                .as("setTargetVertexId patches in the now-known downstream JobVertexID")
                .isEqualTo(downstreamId);
    }

    @Test
    void testEqualsAndHashCodeAreInvariantUnderTargetVertexIdPatch() {
        IntermediateDataSetID dataSetId = new IntermediateDataSetID();
        NonChainedOutput a = newOutput(dataSetId, null);
        NonChainedOutput b = newOutput(dataSetId, null);

        assertThat(a).isEqualTo(b);
        assertThat(a).hasSameHashCodeAs(b);

        a.setTargetVertexId(new JobVertexID());

        assertThat(a)
                .as(
                        "equality is keyed by dataSetId only, so patching the target id must not"
                                + " change equals/hashCode semantics")
                .isEqualTo(b);
        assertThat(a).hasSameHashCodeAs(b);
    }

    private static NonChainedOutput newOutput(JobVertexID targetVertexId) {
        return newOutput(new IntermediateDataSetID(), targetVertexId);
    }

    private static NonChainedOutput newOutput(
            IntermediateDataSetID dataSetId, JobVertexID targetVertexId) {
        return new NonChainedOutput(
                /* supportsUnalignedCheckpoints */ true,
                /* sourceNodeId */ 0,
                targetVertexId,
                /* consumerParallelism */ 1,
                /* consumerMaxParallelism */ 1,
                /* bufferTimeout */ 0L,
                /* isPersistentDataSet */ false,
                dataSetId,
                /* outputTag */ null,
                new ForwardPartitioner<>(),
                ResultPartitionType.PIPELINED_BOUNDED);
    }
}
