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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.job.SubtasksAllAccumulatorsHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collection;
import java.util.Objects;

/** Response type of the {@link SubtasksAllAccumulatorsHandler}. */
public class SubtasksAllAccumulatorsInfo implements ResponseBody {

    public static final String FIELD_NAME_JOB_VERTEX_ID = "id";
    public static final String FIELD_NAME_PARALLELISM = "parallelism";
    public static final String FIELD_NAME_SUBTASKS = "subtasks";

    @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
    @JsonSerialize(using = JobVertexIDSerializer.class)
    private final JobVertexID jobVertexId;

    @JsonProperty(FIELD_NAME_PARALLELISM)
    private final int parallelism;

    @JsonProperty(FIELD_NAME_SUBTASKS)
    private final Collection<SubtaskAccumulatorsInfo> subtaskAccumulatorsInfos;

    @JsonCreator
    public SubtasksAllAccumulatorsInfo(
            @JsonDeserialize(using = JobVertexIDDeserializer.class)
                    @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
                    JobVertexID jobVertexId,
            @JsonProperty(FIELD_NAME_PARALLELISM) int parallelism,
            @JsonProperty(FIELD_NAME_SUBTASKS)
                    Collection<SubtaskAccumulatorsInfo> subtaskAccumulatorsInfos) {
        this.jobVertexId = Preconditions.checkNotNull(jobVertexId);
        this.parallelism = parallelism;
        this.subtaskAccumulatorsInfos = Preconditions.checkNotNull(subtaskAccumulatorsInfos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubtasksAllAccumulatorsInfo that = (SubtasksAllAccumulatorsInfo) o;
        return Objects.equals(jobVertexId, that.jobVertexId)
                && parallelism == that.parallelism
                && Objects.equals(subtaskAccumulatorsInfos, that.subtaskAccumulatorsInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobVertexId, parallelism, subtaskAccumulatorsInfos);
    }

    // ---------------------------------------------------
    // Static inner classes
    // ---------------------------------------------------

    /** Detailed information about subtask accumulators. */
    public static class SubtaskAccumulatorsInfo {
        public static final String FIELD_NAME_SUBTASK_INDEX = "subtask";
        public static final String FIELD_NAME_ATTEMPT_NUM = "attempt";
        public static final String FIELD_NAME_HOST = "host";
        public static final String FIELD_NAME_USER_ACCUMULATORS = "user-accumulators";

        @JsonProperty(FIELD_NAME_SUBTASK_INDEX)
        private final int subtaskIndex;

        @JsonProperty(FIELD_NAME_ATTEMPT_NUM)
        private final int attemptNum;

        @JsonProperty(FIELD_NAME_HOST)
        private final String host;

        @JsonProperty(FIELD_NAME_USER_ACCUMULATORS)
        private final Collection<UserAccumulator> userAccumulators;

        @JsonCreator
        public SubtaskAccumulatorsInfo(
                @JsonProperty(FIELD_NAME_SUBTASK_INDEX) int subtaskIndex,
                @JsonProperty(FIELD_NAME_ATTEMPT_NUM) int attemptNum,
                @JsonProperty(FIELD_NAME_HOST) String host,
                @JsonProperty(FIELD_NAME_USER_ACCUMULATORS)
                        Collection<UserAccumulator> userAccumulators) {

            this.subtaskIndex = subtaskIndex;
            this.attemptNum = attemptNum;
            this.host = Preconditions.checkNotNull(host);
            this.userAccumulators = Preconditions.checkNotNull(userAccumulators);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubtaskAccumulatorsInfo that = (SubtaskAccumulatorsInfo) o;
            return subtaskIndex == that.subtaskIndex
                    && attemptNum == that.attemptNum
                    && Objects.equals(host, that.host)
                    && Objects.equals(userAccumulators, that.userAccumulators);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subtaskIndex, attemptNum, host, userAccumulators);
        }
    }
}
