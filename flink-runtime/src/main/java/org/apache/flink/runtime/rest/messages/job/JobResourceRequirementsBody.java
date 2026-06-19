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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.annotation.docs.FlinkJsonSchema;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeyDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeySerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAnyGetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAnySetter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Body for change job requests. */
@FlinkJsonSchema.AdditionalFields(type = JobVertexResourceRequirements.class)
public class JobResourceRequirementsBody implements RequestBody, ResponseBody {

    @JsonAnySetter
    @JsonAnyGetter
    @JsonSerialize(keyUsing = JobVertexIDKeySerializer.class)
    @JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class)
    private final Map<JobVertexID, JobVertexResourceRequirements> jobVertexResourceRequirements;

    public JobResourceRequirementsBody() {
        this(null);
    }

    public JobResourceRequirementsBody(@Nullable JobResourceRequirements jobResourceRequirements) {
        if (jobResourceRequirements != null) {
            this.jobVertexResourceRequirements = jobResourceRequirements.getJobVertexParallelisms();
        } else {
            this.jobVertexResourceRequirements = new HashMap<>();
        }
    }

    @JsonIgnore
    public Optional<JobResourceRequirements> asJobResourceRequirements() {
        if (jobVertexResourceRequirements.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new JobResourceRequirements(jobVertexResourceRequirements));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JobResourceRequirementsBody that = (JobResourceRequirementsBody) o;
        return Objects.equals(jobVertexResourceRequirements, that.jobVertexResourceRequirements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobVertexResourceRequirements);
    }
}
