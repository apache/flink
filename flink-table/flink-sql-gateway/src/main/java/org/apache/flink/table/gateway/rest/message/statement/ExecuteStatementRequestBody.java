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

package org.apache.flink.table.gateway.rest.message.statement;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;

/** {@link RequestBody} for executing a statement. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExecuteStatementRequestBody implements RequestBody {

    private static final String FIELD_NAME_STATEMENT = "statement";
    private static final String FIELD_NAME_EXECUTION_TIMEOUT = "executionTimeout";
    private static final String FIELD_NAME_EXECUTION_CONFIG = "executionConfig";

    @JsonProperty(FIELD_NAME_STATEMENT)
    private final String statement;

    @JsonProperty(FIELD_NAME_EXECUTION_TIMEOUT)
    @Nullable
    private final Long timeout;

    @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
    @Nullable
    private final Map<String, String> executionConfig;

    public ExecuteStatementRequestBody(String statement) {
        this(statement, 0L, null);
    }

    @JsonCreator
    public ExecuteStatementRequestBody(
            @JsonProperty(FIELD_NAME_STATEMENT) String statement,
            @Nullable @JsonProperty(FIELD_NAME_EXECUTION_TIMEOUT) Long timeout,
            @Nullable @JsonProperty(FIELD_NAME_EXECUTION_CONFIG)
                    Map<String, String> executionConfig) {
        this.statement = statement;
        this.timeout = timeout;
        this.executionConfig = executionConfig;
    }

    public String getStatement() {
        return statement;
    }

    @Nullable
    public Long getTimeout() {
        return timeout;
    }

    @Nullable
    public Map<String, String> getExecutionConfig() {
        return executionConfig;
    }
}
