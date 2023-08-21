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

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/** Represents information about all pending savepoints. */
public class SavepointDumpInfo implements ResponseBody {
    private static final String FILED_NAME_PENDING_IDS = "pending-ids";

    @JsonProperty(FILED_NAME_PENDING_IDS)
    @Nullable
    private final List<String> pendingSavepointIds;

    @JsonCreator
    public SavepointDumpInfo(
            @JsonProperty(FILED_NAME_PENDING_IDS) @Nullable List<String> pendingSavepointIds) {
        this.pendingSavepointIds = pendingSavepointIds;
    }

    @Nullable
    public List<String> getPendingSavepointIds() {
        return pendingSavepointIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SavepointDumpInfo that = (SavepointDumpInfo) o;
        return Objects.equals(pendingSavepointIds, that.pendingSavepointIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pendingSavepointIds);
    }
}
