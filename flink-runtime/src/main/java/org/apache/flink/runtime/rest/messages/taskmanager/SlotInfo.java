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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.rest.messages.ResourceProfileInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.Objects;

/**
 * Class containing information for a slot of {@link
 * org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerSlotInformation}.
 */
public class SlotInfo implements ResponseBody, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_RESOURCE = "resource";

    public static final String FIELD_NAME_JOB_ID = "jobId";

    public static final String FIELD_NAME_NUMBER_OF_TASKS = "numberOfTasks";

    @JsonProperty(FIELD_NAME_RESOURCE)
    private final ResourceProfileInfo resource;

    @JsonProperty(FIELD_NAME_JOB_ID)
    @JsonSerialize(using = JobIDSerializer.class)
    private final JobID jobId;

    @JsonProperty(FIELD_NAME_NUMBER_OF_TASKS)
    private final int numberOfTasks;

    @JsonCreator
    public SlotInfo(
            @JsonDeserialize(using = JobIDDeserializer.class) @JsonProperty(FIELD_NAME_JOB_ID)
                    JobID jobId,
            @JsonProperty(FIELD_NAME_RESOURCE) ResourceProfileInfo resource,
            @JsonProperty(FIELD_NAME_NUMBER_OF_TASKS) int numberOfTasks) {
        this.jobId = Preconditions.checkNotNull(jobId);
        this.resource = Preconditions.checkNotNull(resource);
        this.numberOfTasks = numberOfTasks;
    }

    public SlotInfo(JobID jobId, ResourceProfile resource, int numberOfTasks) {
        this(jobId, ResourceProfileInfo.fromResourceProfile(resource), numberOfTasks);
    }

    @JsonIgnore
    public JobID getJobId() {
        return jobId;
    }

    @JsonIgnore
    public int getNumberOfTasks() {
        return numberOfTasks;
    }

    @JsonIgnore
    public ResourceProfileInfo getResource() {
        return resource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotInfo that = (SlotInfo) o;
        return Objects.equals(jobId, that.jobId)
                && Objects.equals(resource, that.resource)
                && Objects.equals(numberOfTasks, that.numberOfTasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, resource, numberOfTasks);
    }
}
