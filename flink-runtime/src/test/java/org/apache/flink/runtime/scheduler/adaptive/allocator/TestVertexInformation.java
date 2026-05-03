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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestVertexInformation implements JobInformation.VertexInformation {

    private final JobVertexID jobVertexId;
    private final int minParallelism;
    private final int parallelism;
    private final SlotSharingGroup slotSharingGroup;
    @Nullable private final TestingCoLocationGroup coLocationGroup;

    public TestVertexInformation(int parallelism, SlotSharingGroup slotSharingGroup) {
        this(new JobVertexID(), 1, parallelism, slotSharingGroup);
    }

    public TestVertexInformation(
            int parallelism,
            SlotSharingGroup slotSharingGroup,
            TestingCoLocationGroup coLocationGroup) {
        this(new JobVertexID(), 1, parallelism, slotSharingGroup, coLocationGroup);
    }

    TestVertexInformation(
            JobVertexID jobVertexId, int parallelism, SlotSharingGroup slotSharingGroup) {
        this(jobVertexId, 1, parallelism, slotSharingGroup);
    }

    TestVertexInformation(
            JobVertexID jobVertexId,
            int minParallelism,
            int parallelism,
            SlotSharingGroup slotSharingGroup) {
        this(jobVertexId, minParallelism, parallelism, slotSharingGroup, null);
    }

    TestVertexInformation(
            JobVertexID jobVertexId,
            int minParallelism,
            int parallelism,
            SlotSharingGroup slotSharingGroup,
            @Nullable TestingCoLocationGroup coLocationGroup) {
        this.jobVertexId = jobVertexId;
        this.minParallelism = minParallelism;
        this.parallelism = parallelism;
        this.slotSharingGroup = slotSharingGroup;
        this.slotSharingGroup.addVertexToGroup(jobVertexId);
        this.coLocationGroup = coLocationGroup;
        if (this.coLocationGroup != null) {
            this.coLocationGroup.addVertex(jobVertexId);
        }
    }

    @Override
    public JobVertexID getJobVertexID() {
        return jobVertexId;
    }

    @Override
    public String getVertexName() {
        return "JobVertex-" + jobVertexId.toString();
    }

    @Override
    public int getMinParallelism() {
        return minParallelism;
    }

    @Override
    public int getParallelism() {
        return parallelism;
    }

    @Override
    public int getMaxParallelism() {
        return 128;
    }

    @Override
    public SlotSharingGroup getSlotSharingGroup() {
        return slotSharingGroup;
    }

    @Nullable
    @Override
    public TestingCoLocationGroup getCoLocationGroup() {
        return coLocationGroup;
    }

    /** Testing util class. */
    public static class TestingCoLocationGroup extends CoLocationGroupImpl {
        private final List<JobVertexID> verticesIDs = new ArrayList<>();

        public TestingCoLocationGroup(JobVertexID... verticesIDs) {
            super();
            if (verticesIDs != null && verticesIDs.length > 0) {
                this.verticesIDs.addAll(Arrays.stream(verticesIDs).collect(Collectors.toList()));
            }
        }

        public void addVertex(JobVertexID vertexID) {
            this.verticesIDs.add(vertexID);
        }

        @Override
        public List<JobVertexID> getVertexIds() {
            return verticesIDs;
        }
    }
}
