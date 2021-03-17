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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.Collection;

/** {@link JobInformation} created from a {@link JobGraph}. */
public class JobGraphJobInformation implements JobInformation {

    private final JobGraph jobGraph;
    private final JobID jobID;
    private final String name;

    public JobGraphJobInformation(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
        this.jobID = jobGraph.getJobID();
        this.name = jobGraph.getName();
    }

    @Override
    public Collection<SlotSharingGroup> getSlotSharingGroups() {
        return jobGraph.getSlotSharingGroups();
    }

    @Override
    public JobInformation.VertexInformation getVertexInformation(JobVertexID jobVertexId) {
        return new JobVertexInformation(jobGraph.findVertexByID(jobVertexId));
    }

    public JobID getJobID() {
        return jobID;
    }

    public String getName() {
        return name;
    }

    public Iterable<JobInformation.VertexInformation> getVertices() {
        return jobGraphVerticesToVertexInformation(jobGraph.getVertices());
    }

    public static Iterable<JobInformation.VertexInformation> jobGraphVerticesToVertexInformation(
            Iterable<JobVertex> verticesIterable) {
        return Iterables.transform(verticesIterable, JobVertexInformation::new);
    }

    /** Returns a copy of a jobGraph that can be mutated. */
    public JobGraph copyJobGraph() throws IOException, ClassNotFoundException {
        return InstantiationUtil.clone(jobGraph);
    }

    private static final class JobVertexInformation implements JobInformation.VertexInformation {

        private final JobVertex jobVertex;

        private JobVertexInformation(JobVertex jobVertex) {
            this.jobVertex = jobVertex;
        }

        @Override
        public JobVertexID getJobVertexID() {
            return jobVertex.getID();
        }

        @Override
        public int getParallelism() {
            return jobVertex.getParallelism();
        }

        @Override
        public SlotSharingGroup getSlotSharingGroup() {
            return jobVertex.getSlotSharingGroup();
        }
    }
}
