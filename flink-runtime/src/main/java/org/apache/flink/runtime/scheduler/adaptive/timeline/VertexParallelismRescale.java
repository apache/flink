/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** The rescale information of a {@link org.apache.flink.runtime.jobgraph.JobVertex}. */
public class VertexParallelismRescale implements Serializable {

    private static final long serialVersionUID = 1L;

    private final JobVertexID jobVertexId;
    private String jobVertexName;
    private SlotSharingGroupId slotSharingGroupId;
    private String slotSharingGroupName;
    private Integer desiredParallelism;
    private Integer sufficientParallelism;

    @Nullable private Integer preRescaleParallelism;

    @Nullable private Integer postRescaleParallelism;

    public VertexParallelismRescale(
            JobVertexID jobVertexId, String jobVertexName, SlotSharingGroup slotSharingGroup) {
        this.jobVertexId = Preconditions.checkNotNull(jobVertexId);
        this.jobVertexName = jobVertexName;
        this.slotSharingGroupName = slotSharingGroup.getSlotSharingGroupName();
        this.slotSharingGroupId = slotSharingGroup.getSlotSharingGroupId();
    }

    public JobVertexID getJobVertexId() {
        return jobVertexId;
    }

    public String getJobVertexName() {
        return jobVertexName;
    }

    public void setJobVertexName(String jobVertexName) {
        this.jobVertexName = jobVertexName;
    }

    public SlotSharingGroupId getSlotSharingGroupId() {
        return slotSharingGroupId;
    }

    public String getSlotSharingGroupName() {
        return slotSharingGroupName;
    }

    @Nullable
    public Integer getPreRescaleParallelism() {
        return preRescaleParallelism;
    }

    public void setPreRescaleParallelism(@Nullable Integer preRescaleParallelism) {
        this.preRescaleParallelism = preRescaleParallelism;
    }

    public Integer getDesiredParallelism() {
        return desiredParallelism;
    }

    public Integer getSufficientParallelism() {
        return sufficientParallelism;
    }

    public void setRequiredParallelisms(VertexParallelismInformation vertexParallelismInformation) {
        this.sufficientParallelism = vertexParallelismInformation.getMinParallelism();
        this.desiredParallelism = vertexParallelismInformation.getParallelism();
    }

    @Nullable
    public Integer getPostRescaleParallelism() {
        return postRescaleParallelism;
    }

    public void setPostRescaleParallelism(Integer postRescaleParallelism) {
        this.postRescaleParallelism = postRescaleParallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VertexParallelismRescale that = (VertexParallelismRescale) o;
        return Objects.equals(jobVertexId, that.jobVertexId)
                && Objects.equals(jobVertexName, that.jobVertexName)
                && Objects.equals(slotSharingGroupId, that.slotSharingGroupId)
                && Objects.equals(slotSharingGroupName, that.slotSharingGroupName)
                && Objects.equals(preRescaleParallelism, that.preRescaleParallelism)
                && Objects.equals(desiredParallelism, that.desiredParallelism)
                && Objects.equals(sufficientParallelism, that.sufficientParallelism)
                && Objects.equals(postRescaleParallelism, that.postRescaleParallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jobVertexId,
                jobVertexName,
                slotSharingGroupId,
                slotSharingGroupName,
                preRescaleParallelism,
                desiredParallelism,
                sufficientParallelism,
                postRescaleParallelism);
    }

    @Override
    public String toString() {
        return "VertexParallelismRescale{"
                + "jobVertexId="
                + jobVertexId
                + ", jobVertexName='"
                + jobVertexName
                + '\''
                + ", slotSharingGroupId="
                + slotSharingGroupId
                + ", slotSharingGroupName='"
                + slotSharingGroupName
                + '\''
                + ", desiredParallelism="
                + desiredParallelism
                + ", sufficientParallelism="
                + sufficientParallelism
                + ", preRescaleParallelism="
                + preRescaleParallelism
                + ", postRescaleParallelism="
                + postRescaleParallelism
                + '}';
    }
}
