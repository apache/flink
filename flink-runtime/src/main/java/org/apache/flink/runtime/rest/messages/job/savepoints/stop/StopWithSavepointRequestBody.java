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

package org.apache.flink.runtime.rest.messages.job.savepoints.stop;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.TriggerId;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Optional;

/** {@link RequestBody} for stopping a job with a savepoint. */
public class StopWithSavepointRequestBody implements RequestBody {

    public static final String FIELD_NAME_TARGET_DIRECTORY = "targetDirectory";

    public static final String FIELD_NAME_FORMAT_TYPE = "formatType";

    private static final String FIELD_NAME_DRAIN = "drain";

    private static final String FIELD_NAME_TRIGGER_ID = "triggerId";

    @JsonProperty(FIELD_NAME_TARGET_DIRECTORY)
    @Nullable
    private final String targetDirectory;

    @JsonProperty(FIELD_NAME_DRAIN)
    private final boolean drain;

    @JsonProperty(FIELD_NAME_TRIGGER_ID)
    @Nullable
    private final TriggerId triggerId;

    @JsonProperty(FIELD_NAME_FORMAT_TYPE)
    @Nullable
    private final SavepointFormatType formatType;

    @JsonCreator
    public StopWithSavepointRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_TARGET_DIRECTORY) final String targetDirectory,
            @Nullable @JsonProperty(FIELD_NAME_DRAIN) final Boolean drain,
            @Nullable @JsonProperty(FIELD_NAME_FORMAT_TYPE) final SavepointFormatType formatType,
            @Nullable @JsonProperty(FIELD_NAME_TRIGGER_ID) TriggerId triggerId) {
        this.targetDirectory = targetDirectory;
        this.drain = drain != null ? drain : false;
        this.triggerId = triggerId;
        this.formatType = formatType != null ? formatType : SavepointFormatType.DEFAULT;
    }

    @JsonIgnore
    public Optional<String> getTargetDirectory() {
        return Optional.ofNullable(targetDirectory);
    }

    public boolean shouldDrain() {
        return drain;
    }

    @JsonIgnore
    public Optional<TriggerId> getTriggerId() {
        return Optional.ofNullable(triggerId);
    }

    @JsonIgnore
    public SavepointFormatType getFormatType() {
        return formatType;
    }
}
