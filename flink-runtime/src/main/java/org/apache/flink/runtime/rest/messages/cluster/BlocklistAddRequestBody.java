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

package org.apache.flink.runtime.rest.messages.cluster;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;

/** Request body for adding a node to the management blocklist. */
public class BlocklistAddRequestBody implements RequestBody {

    public static final String FIELD_NAME_NODE_ID = "nodeId";
    public static final String FIELD_NAME_REASON = "reason";
    public static final String FIELD_NAME_DURATION = "duration";

    @JsonProperty(FIELD_NAME_NODE_ID)
    private final String nodeId;

    @JsonProperty(FIELD_NAME_REASON)
    private final String reason;

    @JsonProperty(FIELD_NAME_DURATION)
    @Nullable
    private final Duration duration;

    @JsonCreator
    public BlocklistAddRequestBody(
            @JsonProperty(FIELD_NAME_NODE_ID) String nodeId,
            @JsonProperty(FIELD_NAME_REASON) String reason,
            @JsonProperty(FIELD_NAME_DURATION) @Nullable Duration duration) {
        this.nodeId = Preconditions.checkNotNull(nodeId, "nodeId must not be null");
        this.reason = Preconditions.checkNotNull(reason, "reason must not be null");
        this.duration = duration;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getReason() {
        return reason;
    }

    @Nullable
    public Duration getDuration() {
        return duration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlocklistAddRequestBody that = (BlocklistAddRequestBody) o;
        return Objects.equals(nodeId, that.nodeId)
                && Objects.equals(reason, that.reason)
                && Objects.equals(duration, that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, reason, duration);
    }

    @Override
    public String toString() {
        return "BlocklistAddRequestBody{"
                + "nodeId='"
                + nodeId
                + '\''
                + ", reason='"
                + reason
                + '\''
                + ", duration="
                + duration
                + '}';
    }
}
