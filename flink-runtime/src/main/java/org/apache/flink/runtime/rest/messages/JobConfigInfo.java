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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.rest.handler.job.JobConfigHandler;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/** Response type of the {@link JobConfigHandler}. */
@JsonSerialize(using = JobConfigInfo.Serializer.class)
@JsonDeserialize(using = JobConfigInfo.Deserializer.class)
public class JobConfigInfo implements ResponseBody {

    public static final String FIELD_NAME_JOB_ID = "jid";
    public static final String FIELD_NAME_JOB_NAME = "name";
    public static final String FIELD_NAME_EXECUTION_CONFIG = "execution-config";

    private final JobID jobId;

    private final String jobName;

    @Nullable private final ExecutionConfigInfo executionConfigInfo;

    public JobConfigInfo(
            JobID jobId, String jobName, @Nullable ExecutionConfigInfo executionConfigInfo) {
        this.jobId = Preconditions.checkNotNull(jobId);
        this.jobName = Preconditions.checkNotNull(jobName);
        this.executionConfigInfo = executionConfigInfo;
    }

    public JobID getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    @Nullable
    public ExecutionConfigInfo getExecutionConfigInfo() {
        return executionConfigInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobConfigInfo that = (JobConfigInfo) o;
        return Objects.equals(jobId, that.jobId)
                && Objects.equals(jobName, that.jobName)
                && Objects.equals(executionConfigInfo, that.executionConfigInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobName, executionConfigInfo);
    }

    // ---------------------------------------------------------------------------------
    // Static helper classes
    // ---------------------------------------------------------------------------------

    /** Json serializer for the {@link JobConfigInfo}. */
    public static final class Serializer extends StdSerializer<JobConfigInfo> {

        private static final long serialVersionUID = -1551666039618928811L;

        public Serializer() {
            super(JobConfigInfo.class);
        }

        @Override
        public void serialize(
                JobConfigInfo jobConfigInfo,
                JsonGenerator jsonGenerator,
                SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();

            jsonGenerator.writeStringField(FIELD_NAME_JOB_ID, jobConfigInfo.getJobId().toString());
            jsonGenerator.writeStringField(FIELD_NAME_JOB_NAME, jobConfigInfo.getJobName());

            if (jobConfigInfo.getExecutionConfigInfo() != null) {
                jsonGenerator.writeObjectField(
                        FIELD_NAME_EXECUTION_CONFIG, jobConfigInfo.getExecutionConfigInfo());
            }

            jsonGenerator.writeEndObject();
        }
    }

    /** Json deserializer for the {@link JobConfigInfo}. */
    public static final class Deserializer extends StdDeserializer<JobConfigInfo> {

        private static final long serialVersionUID = -3580088509877177213L;

        public Deserializer() {
            super(JobConfigInfo.class);
        }

        @Override
        public JobConfigInfo deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            JsonNode rootNode = jsonParser.readValueAsTree();

            final JobID jobId = JobID.fromHexString(rootNode.get(FIELD_NAME_JOB_ID).asText());
            final String jobName = rootNode.get(FIELD_NAME_JOB_NAME).asText();

            final ExecutionConfigInfo executionConfigInfo;

            if (rootNode.has(FIELD_NAME_EXECUTION_CONFIG)) {
                executionConfigInfo =
                        RestMapperUtils.getStrictObjectMapper()
                                .treeToValue(
                                        rootNode.get(FIELD_NAME_EXECUTION_CONFIG),
                                        ExecutionConfigInfo.class);
            } else {
                executionConfigInfo = null;
            }

            return new JobConfigInfo(jobId, jobName, executionConfigInfo);
        }
    }

    /** Nested class to encapsulate the execution configuration. */
    public static final class ExecutionConfigInfo {

        public static final String FIELD_NAME_EXECUTION_MODE = "execution-mode";
        public static final String FIELD_NAME_RESTART_STRATEGY = "restart-strategy";
        public static final String FIELD_NAME_PARALLELISM = "job-parallelism";
        public static final String FIELD_NAME_OBJECT_REUSE_MODE = "object-reuse-mode";
        public static final String FIELD_NAME_GLOBAL_JOB_PARAMETERS = "user-config";

        @JsonProperty(FIELD_NAME_EXECUTION_MODE)
        private final String executionMode;

        @JsonProperty(FIELD_NAME_RESTART_STRATEGY)
        private final String restartStrategy;

        @JsonProperty(FIELD_NAME_PARALLELISM)
        private final int parallelism;

        @JsonProperty(FIELD_NAME_OBJECT_REUSE_MODE)
        private final boolean isObjectReuse;

        @JsonProperty(FIELD_NAME_GLOBAL_JOB_PARAMETERS)
        private final Map<String, String> globalJobParameters;

        @JsonCreator
        public ExecutionConfigInfo(
                @JsonProperty(FIELD_NAME_EXECUTION_MODE) String executionMode,
                @JsonProperty(FIELD_NAME_RESTART_STRATEGY) String restartStrategy,
                @JsonProperty(FIELD_NAME_PARALLELISM) int parallelism,
                @JsonProperty(FIELD_NAME_OBJECT_REUSE_MODE) boolean isObjectReuse,
                @JsonProperty(FIELD_NAME_GLOBAL_JOB_PARAMETERS)
                        Map<String, String> globalJobParameters) {
            this.executionMode = Preconditions.checkNotNull(executionMode);
            this.restartStrategy = Preconditions.checkNotNull(restartStrategy);
            this.parallelism = parallelism;
            this.isObjectReuse = isObjectReuse;
            this.globalJobParameters = Preconditions.checkNotNull(globalJobParameters);
        }

        public String getExecutionMode() {
            return executionMode;
        }

        public String getRestartStrategy() {
            return restartStrategy;
        }

        public int getParallelism() {
            return parallelism;
        }

        @JsonIgnore
        public boolean isObjectReuse() {
            return isObjectReuse;
        }

        public Map<String, String> getGlobalJobParameters() {
            return globalJobParameters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExecutionConfigInfo that = (ExecutionConfigInfo) o;
            return parallelism == that.parallelism
                    && isObjectReuse == that.isObjectReuse
                    && Objects.equals(executionMode, that.executionMode)
                    && Objects.equals(restartStrategy, that.restartStrategy)
                    && Objects.equals(globalJobParameters, that.globalJobParameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    executionMode,
                    restartStrategy,
                    parallelism,
                    isObjectReuse,
                    globalJobParameters);
        }

        public static ExecutionConfigInfo from(ArchivedExecutionConfig archivedExecutionConfig) {
            return new ExecutionConfigInfo(
                    archivedExecutionConfig.getExecutionMode(),
                    archivedExecutionConfig.getRestartStrategyDescription(),
                    archivedExecutionConfig.getParallelism(),
                    archivedExecutionConfig.getObjectReuseEnabled(),
                    ConfigurationUtils.hideSensitiveValues(
                            archivedExecutionConfig.getGlobalJobParameters()));
        }
    }
}
