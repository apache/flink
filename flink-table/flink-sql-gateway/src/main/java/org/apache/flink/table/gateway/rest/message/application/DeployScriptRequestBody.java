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

package org.apache.flink.table.gateway.rest.message.application;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/** Request to deploy a script in application mode. */
public class DeployScriptRequestBody implements RequestBody {

    private static final String FIELD_NAME_SCRIPT = "script";
    private static final String FIELD_NAME_SCRIPT_URI = "scriptUri";
    private static final String FIELD_NAME_EXECUTION_CONFIG = "executionConfig";

    @JsonProperty(FIELD_NAME_SCRIPT)
    private final @Nullable String script;

    @JsonProperty(FIELD_NAME_SCRIPT_URI)
    private final @Nullable String scriptUri;

    @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
    private final Map<String, String> executionConfig;

    @JsonCreator
    public DeployScriptRequestBody(
            @JsonProperty(FIELD_NAME_SCRIPT) @Nullable String script,
            @JsonProperty(FIELD_NAME_SCRIPT_URI) @Nullable String scriptUri,
            @JsonProperty(FIELD_NAME_EXECUTION_CONFIG) @Nullable
                    Map<String, String> executionConfig) {
        this.script = script;
        this.scriptUri = scriptUri;
        this.executionConfig = executionConfig == null ? Collections.emptyMap() : executionConfig;
    }

    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }

    @Nullable
    public String getScript() {
        return script;
    }

    @Nullable
    public String getScriptUri() {
        return scriptUri;
    }
}
