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

/** {@link RequestBody} for completing a statement. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompleteStatementRequestBody implements RequestBody {
    private static final String FIELD_NAME_STATEMENT = "statement";
    private static final String FIELD_NAME_POSITION = "position";

    @JsonProperty(FIELD_NAME_STATEMENT)
    private final String statement;

    @JsonProperty(FIELD_NAME_POSITION)
    private final int position;

    @JsonCreator
    public CompleteStatementRequestBody(
            @JsonProperty(FIELD_NAME_STATEMENT) String statement,
            @JsonProperty(FIELD_NAME_POSITION) int position) {
        this.statement = statement;
        this.position = position;
    }

    public String getStatement() {
        return statement;
    }

    public int getPosition() {
        return position;
    }
}
