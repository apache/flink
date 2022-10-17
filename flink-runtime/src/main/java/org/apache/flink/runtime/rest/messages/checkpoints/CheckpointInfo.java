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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.SerializedThrowableDeserializer;
import org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Represents information about a triggered checkpoint. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CheckpointInfo implements ResponseBody {

    private static final String FIELD_NAME_CHECKPOINT_ID = "checkpointId";

    private static final String FIELD_NAME_FAILURE_CAUSE = "failureCause";

    @JsonProperty(FIELD_NAME_CHECKPOINT_ID)
    @Nullable
    private final Long checkpointId;

    @JsonProperty(FIELD_NAME_FAILURE_CAUSE)
    @JsonSerialize(using = SerializedThrowableSerializer.class)
    @JsonDeserialize(using = SerializedThrowableDeserializer.class)
    @Nullable
    private final SerializedThrowable failureCause;

    @JsonCreator
    public CheckpointInfo(
            @JsonProperty(FIELD_NAME_CHECKPOINT_ID) @Nullable final Long checkpointId,
            @JsonProperty(FIELD_NAME_FAILURE_CAUSE)
                    @JsonDeserialize(using = SerializedThrowableDeserializer.class)
                    @Nullable
                    final SerializedThrowable failureCause) {
        checkArgument(
                checkpointId != null ^ failureCause != null,
                "Either checkpointId or failureCause must be set");

        this.checkpointId = checkpointId;
        this.failureCause = failureCause;
    }

    @Nullable
    @JsonIgnore
    public Long getCheckpointId() {
        return checkpointId;
    }

    @Nullable
    @JsonIgnore
    public SerializedThrowable getFailureCause() {
        return failureCause;
    }
}
