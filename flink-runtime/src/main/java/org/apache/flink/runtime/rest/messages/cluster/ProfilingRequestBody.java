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

import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** {@link RequestBody} to trigger profiling instance. */
public class ProfilingRequestBody implements RequestBody {

    private static final String FIELD_NAME_MODE = "mode";
    private static final String FIELD_NAME_DURATION = "duration";

    @JsonProperty(FIELD_NAME_MODE)
    private final ProfilingInfo.ProfilingMode mode;

    @JsonProperty(FIELD_NAME_DURATION)
    private final int duration;

    @JsonCreator
    public ProfilingRequestBody(
            @JsonProperty(FIELD_NAME_MODE) final ProfilingInfo.ProfilingMode mode,
            @JsonProperty(FIELD_NAME_DURATION) final int duration) {
        this.mode = mode;
        this.duration = duration;
    }

    @JsonIgnore
    public ProfilingInfo.ProfilingMode getMode() {
        return mode;
    }

    @JsonIgnore
    public int getDuration() {
        return duration;
    }
}
