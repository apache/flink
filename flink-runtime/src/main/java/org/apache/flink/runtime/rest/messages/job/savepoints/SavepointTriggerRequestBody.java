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

package org.apache.flink.runtime.rest.messages.job.savepoints;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** {@link RequestBody} to trigger savepoints. */
public class SavepointTriggerRequestBody implements RequestBody {

    public static final String FIELD_NAME_TARGET_DIRECTORY = "target-directory";

    private static final String FIELD_NAME_CANCEL_JOB = "cancel-job";

    @JsonProperty(FIELD_NAME_TARGET_DIRECTORY)
    @Nullable
    private final String targetDirectory;

    @JsonProperty(FIELD_NAME_CANCEL_JOB)
    private final boolean cancelJob;

    @JsonCreator
    public SavepointTriggerRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_TARGET_DIRECTORY) final String targetDirectory,
            @JsonProperty(value = FIELD_NAME_CANCEL_JOB, defaultValue = "false")
                    final boolean cancelJob) {
        this.targetDirectory = targetDirectory;
        this.cancelJob = cancelJob;
    }

    @Nullable
    public String getTargetDirectory() {
        return targetDirectory;
    }

    public boolean isCancelJob() {
        return cancelJob;
    }
}
