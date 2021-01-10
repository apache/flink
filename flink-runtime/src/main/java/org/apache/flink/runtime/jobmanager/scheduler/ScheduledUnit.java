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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/** ScheduledUnit contains the information necessary to allocate a slot for the given task. */
public class ScheduledUnit {

    private final ExecutionVertexID executionVertexId;

    private final SlotSharingGroupId slotSharingGroupId;

    @Nullable private final CoLocationConstraint coLocationConstraint;

    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    public ScheduledUnit(Execution task) {
        this(task, new SlotSharingGroupId());
    }

    public ScheduledUnit(Execution task, SlotSharingGroupId slotSharingGroupId) {
        this(task, slotSharingGroupId, null);
    }

    public ScheduledUnit(
            Execution task,
            SlotSharingGroupId slotSharingGroupId,
            @Nullable CoLocationConstraint coLocationConstraint) {
        this(
                Preconditions.checkNotNull(task).getVertex().getID(),
                slotSharingGroupId,
                coLocationConstraint);
    }

    @VisibleForTesting
    public ScheduledUnit(
            JobVertexID jobVertexId,
            SlotSharingGroupId slotSharingGroupId,
            @Nullable CoLocationConstraint coLocationConstraint) {
        this(new ExecutionVertexID(jobVertexId, 0), slotSharingGroupId, coLocationConstraint);
    }

    public ScheduledUnit(
            ExecutionVertexID executionVertexId,
            SlotSharingGroupId slotSharingGroupId,
            @Nullable CoLocationConstraint coLocationConstraint) {

        this.executionVertexId = Preconditions.checkNotNull(executionVertexId);
        this.slotSharingGroupId = slotSharingGroupId;
        this.coLocationConstraint = coLocationConstraint;
    }

    // --------------------------------------------------------------------------------------------

    public JobVertexID getJobVertexId() {
        return executionVertexId.getJobVertexId();
    }

    public int getSubtaskIndex() {
        return executionVertexId.getSubtaskIndex();
    }

    public SlotSharingGroupId getSlotSharingGroupId() {
        return slotSharingGroupId;
    }

    @Nullable
    public CoLocationConstraint getCoLocationConstraint() {
        return coLocationConstraint;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return "{task="
                + executionVertexId
                + ", sharingUnit="
                + slotSharingGroupId
                + ", locationConstraint="
                + coLocationConstraint
                + '}';
    }
}
