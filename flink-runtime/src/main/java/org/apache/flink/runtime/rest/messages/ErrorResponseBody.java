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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/** Generic response body for communicating errors on the server. */
public final class ErrorResponseBody implements ResponseBody {

    static final String FIELD_NAME_ERRORS = "errors";
    static final String FIELD_NAME_ROOT_CAUSE = "rootCause";

    @JsonProperty(FIELD_NAME_ERRORS)
    public final List<String> errors;

    @JsonProperty(FIELD_NAME_ROOT_CAUSE)
    public final String rootCause;

    public ErrorResponseBody(String error) {
        this(Collections.singletonList(error), error);
    }

    public ErrorResponseBody(String error, String rootCause) {
        this(Collections.singletonList(error), rootCause);
    }

    @JsonCreator
    public ErrorResponseBody(
            @JsonProperty(FIELD_NAME_ERRORS) List<String> errors,
            @JsonProperty(FIELD_NAME_ROOT_CAUSE) String rootCause) {

        this.errors = errors;
        this.rootCause = rootCause;
    }
}
