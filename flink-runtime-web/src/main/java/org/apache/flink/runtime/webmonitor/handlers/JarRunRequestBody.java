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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** {@link RequestBody} for running a jar. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JarRunRequestBody extends JarRequestBody {
    private static final String FIELD_NAME_ALLOW_NON_RESTORED_STATE = "allowNonRestoredState";

    private static final String FIELD_NAME_SAVEPOINT_PATH = "savepointPath";

    @Deprecated private static final String FIELD_NAME_SAVEPOINT_RESTORE_MODE = "restoreMode";

    private static final String FIELD_NAME_SAVEPOINT_CLAIM_MODE = "claimMode";

    @JsonProperty(FIELD_NAME_ALLOW_NON_RESTORED_STATE)
    @Nullable
    private Boolean allowNonRestoredState;

    @JsonProperty(FIELD_NAME_SAVEPOINT_PATH)
    @Nullable
    private String savepointPath;

    @JsonProperty(FIELD_NAME_SAVEPOINT_RESTORE_MODE)
    @Nullable
    @Deprecated
    @Documentation.ExcludeFromDocumentation
    private RecoveryClaimMode deprecatedRecoveryClaimMode;

    @JsonProperty(FIELD_NAME_SAVEPOINT_CLAIM_MODE)
    @Nullable
    private RecoveryClaimMode recoveryClaimMode;

    public JarRunRequestBody() {
        this(null, null, null, null, null, null, null, null, null);
    }

    /** Fallback constructor ONLY for tests. */
    @VisibleForTesting
    public JarRunRequestBody(
            @Nullable String entryClassName,
            @Nullable List<String> programArgumentsList,
            @Nullable Integer parallelism,
            @Nullable JobID jobId,
            @Nullable Boolean allowNonRestoredState,
            @Nullable String savepointPath,
            @Nullable RecoveryClaimMode recoveryClaimMode,
            @Nullable Map<String, String> flinkConfiguration) {
        this(
                entryClassName,
                programArgumentsList,
                parallelism,
                jobId,
                allowNonRestoredState,
                savepointPath,
                null,
                recoveryClaimMode,
                flinkConfiguration);
    }

    @JsonCreator
    public JarRunRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_ENTRY_CLASS) String entryClassName,
            @Nullable @JsonProperty(FIELD_NAME_PROGRAM_ARGUMENTS_LIST)
                    List<String> programArgumentsList,
            @Nullable @JsonProperty(FIELD_NAME_PARALLELISM) Integer parallelism,
            @Nullable @JsonProperty(FIELD_NAME_JOB_ID) JobID jobId,
            @Nullable @JsonProperty(FIELD_NAME_ALLOW_NON_RESTORED_STATE)
                    Boolean allowNonRestoredState,
            @Nullable @JsonProperty(FIELD_NAME_SAVEPOINT_PATH) String savepointPath,
            @Nullable @JsonProperty(FIELD_NAME_SAVEPOINT_RESTORE_MODE)
                    RecoveryClaimMode deprecatedRecoveryClaimMode,
            @Nullable @JsonProperty(FIELD_NAME_SAVEPOINT_CLAIM_MODE)
                    RecoveryClaimMode recoveryClaimMode,
            @Nullable @JsonProperty(FIELD_NAME_FLINK_CONFIGURATION)
                    Map<String, String> flinkConfiguration) {
        super(entryClassName, programArgumentsList, parallelism, jobId, flinkConfiguration);
        this.allowNonRestoredState = allowNonRestoredState;
        this.savepointPath = savepointPath;
        this.deprecatedRecoveryClaimMode = deprecatedRecoveryClaimMode;
        this.recoveryClaimMode = recoveryClaimMode;
    }

    @Nullable
    @JsonIgnore
    public Boolean getAllowNonRestoredState() {
        return allowNonRestoredState;
    }

    @Nullable
    @JsonIgnore
    public String getSavepointPath() {
        return savepointPath;
    }

    @Nullable
    @JsonIgnore
    public RecoveryClaimMode getRecoveryClaimMode() {
        return recoveryClaimMode == null ? deprecatedRecoveryClaimMode : recoveryClaimMode;
    }

    @JsonIgnore
    public boolean isDeprecatedRestoreModeHasValue() {
        return deprecatedRecoveryClaimMode != null;
    }
}
