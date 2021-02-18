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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests creating and initializing {@link SlotSharingGroup}. */
public class VertexSlotSharingTest {

    /**
     * Test setup: - v1 is isolated, no slot sharing. - v2 and v3 (not connected) share slots. - v4
     * and v5 (connected) share slots.
     */
    @Test
    public void testAssignSlotSharingGroup() {
        try {
            JobVertex v1 = new JobVertex("v1");
            JobVertex v2 = new JobVertex("v2");
            JobVertex v3 = new JobVertex("v3");
            JobVertex v4 = new JobVertex("v4");
            JobVertex v5 = new JobVertex("v5");

            v1.setParallelism(4);
            v2.setParallelism(5);
            v3.setParallelism(7);
            v4.setParallelism(1);
            v5.setParallelism(11);

            v1.setInvokableClass(AbstractInvokable.class);
            v2.setInvokableClass(AbstractInvokable.class);
            v3.setInvokableClass(AbstractInvokable.class);
            v4.setInvokableClass(AbstractInvokable.class);
            v5.setInvokableClass(AbstractInvokable.class);

            v2.connectNewDataSetAsInput(
                    v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            v5.connectNewDataSetAsInput(
                    v4, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

            SlotSharingGroup jg1 = new SlotSharingGroup();
            v2.setSlotSharingGroup(jg1);
            v3.setSlotSharingGroup(jg1);

            SlotSharingGroup jg2 = new SlotSharingGroup();
            v4.setSlotSharingGroup(jg2);
            v5.setSlotSharingGroup(jg2);

            List<JobVertex> vertices = new ArrayList<>(Arrays.asList(v1, v2, v3, v4, v5));

            ExecutionGraph eg = TestingDefaultExecutionGraphBuilder.newBuilder().build();
            eg.attachJobGraph(vertices);

            // verify that the vertices are all in the same slot sharing group
            SlotSharingGroup group1;
            SlotSharingGroup group2;

            // verify that v1 tasks are not in the same slot sharing group as v2
            assertNotEquals(
                    eg.getJobVertex(v1.getID()).getSlotSharingGroup(),
                    eg.getJobVertex(v2.getID()).getSlotSharingGroup());

            // v2 and v3 are shared
            group1 = eg.getJobVertex(v2.getID()).getSlotSharingGroup();
            assertNotNull(group1);
            assertEquals(group1, eg.getJobVertex(v3.getID()).getSlotSharingGroup());

            assertEquals(2, group1.getJobVertexIds().size());
            assertTrue(group1.getJobVertexIds().contains(v2.getID()));
            assertTrue(group1.getJobVertexIds().contains(v3.getID()));

            // v4 and v5 are shared
            group2 = eg.getJobVertex(v4.getID()).getSlotSharingGroup();
            assertNotNull(group2);
            assertEquals(group2, eg.getJobVertex(v5.getID()).getSlotSharingGroup());

            assertEquals(2, group1.getJobVertexIds().size());
            assertTrue(group2.getJobVertexIds().contains(v4.getID()));
            assertTrue(group2.getJobVertexIds().contains(v5.getID()));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
