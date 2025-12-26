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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link RequestBody} for running a jar as an application.
 *
 * <p>Nearly identical to {@link JarRunRequestBody}, but includes additional fields specific to for
 * application and omits deprecated fields.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JarRunApplicationRequestBody extends JarRequestBody {
    private static final String FIELD_NAME_ALLOW_NON_RESTORED_STATE = "allowNonRestoredState";

    private static final String FIELD_NAME_SAVEPOINT_PATH = "savepointPath";

    private static final String FIELD_NAME_SAVEPOINT_CLAIM_MODE = "claimMode";

    private static final String FIELD_NAME_APPLICATION_ID = "applicationId";

    @JsonProperty(FIELD_NAME_ALLOW_NON_RESTORED_STATE)
    @Nullable
    private final Boolean allowNonRestoredState;

    @JsonProperty(FIELD_NAME_SAVEPOINT_PATH)
    @Nullable
    private final String savepointPath;

    @JsonProperty(FIELD_NAME_SAVEPOINT_CLAIM_MODE)
    @Nullable
    private final RecoveryClaimMode recoveryClaimMode;

    @JsonProperty(FIELD_NAME_APPLICATION_ID)
    @JsonDeserialize(using = ApplicationIDDeserializer.class)
    @JsonSerialize(using = ApplicationIDSerializer.class)
    @Nullable
    private final ApplicationID applicationId;

    public JarRunApplicationRequestBody() {
        this(null, null, null, null, null, null, null, null, null);
    }

    @JsonCreator
    public JarRunApplicationRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_ENTRY_CLASS) String entryClassName,
            @Nullable @JsonProperty(FIELD_NAME_PROGRAM_ARGUMENTS_LIST)
                    List<String> programArgumentsList,
            @Nullable @JsonProperty(FIELD_NAME_PARALLELISM) Integer parallelism,
            @Nullable @JsonProperty(FIELD_NAME_JOB_ID) JobID jobId,
            @Nullable @JsonProperty(FIELD_NAME_ALLOW_NON_RESTORED_STATE)
                    Boolean allowNonRestoredState,
            @Nullable @JsonProperty(FIELD_NAME_SAVEPOINT_PATH) String savepointPath,
            @Nullable @JsonProperty(FIELD_NAME_SAVEPOINT_CLAIM_MODE)
                    RecoveryClaimMode recoveryClaimMode,
            @Nullable @JsonProperty(FIELD_NAME_FLINK_CONFIGURATION)
                    Map<String, String> flinkConfiguration,
            @Nullable @JsonProperty(FIELD_NAME_APPLICATION_ID) ApplicationID applicationId) {
        super(entryClassName, programArgumentsList, parallelism, jobId, flinkConfiguration);
        this.allowNonRestoredState = allowNonRestoredState;
        this.savepointPath = savepointPath;
        this.recoveryClaimMode = recoveryClaimMode;
        this.applicationId = applicationId;
    }

    @JsonIgnore
    public Optional<Boolean> getAllowNonRestoredState() {
        return Optional.ofNullable(allowNonRestoredState);
    }

    @JsonIgnore
    public Optional<String> getSavepointPath() {
        return Optional.ofNullable(savepointPath);
    }

    @JsonIgnore
    public Optional<RecoveryClaimMode> getRecoveryClaimMode() {
        return Optional.ofNullable(recoveryClaimMode);
    }

    @JsonIgnore
    public Optional<ApplicationID> getApplicationId() {
        return Optional.ofNullable(applicationId);
    }
}
