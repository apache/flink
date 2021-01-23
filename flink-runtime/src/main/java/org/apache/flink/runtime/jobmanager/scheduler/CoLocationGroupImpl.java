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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator4.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A {@link CoLocationGroup} implementation. */
public class CoLocationGroupImpl implements CoLocationGroup, java.io.Serializable {

    private static final long serialVersionUID = -2605819490401895297L;

    private final AbstractID id = new AbstractID();

    private final List<JobVertex> vertices = new ArrayList<>();

    // --------------------------------------------------------------------------------------------

    public CoLocationGroupImpl(JobVertex... vertices) {
        Collections.addAll(this.vertices, vertices);
    }

    // --------------------------------------------------------------------------------------------

    public void addVertex(JobVertex vertex) {
        Preconditions.checkNotNull(vertex);
        this.vertices.add(vertex);
    }

    @Override
    public List<JobVertexID> getVertexIds() {
        return vertices.stream().map(JobVertex::getID).collect(ImmutableList.toImmutableList());
    }

    @Override
    public CoLocationConstraint getLocationConstraint(int subTaskIndex) {
        return new CoLocationConstraint(id, subTaskIndex);
    }

    public void mergeInto(CoLocationGroupImpl other) {
        Preconditions.checkNotNull(other);

        for (JobVertex v : this.vertices) {
            v.updateCoLocationGroup(other);
        }

        // move vertex membership
        other.vertices.addAll(this.vertices);
        this.vertices.clear();
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public AbstractID getId() {
        return id;
    }
}
