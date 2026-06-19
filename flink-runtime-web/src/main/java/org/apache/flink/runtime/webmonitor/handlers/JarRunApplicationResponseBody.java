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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.ApplicationIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import static java.util.Objects.requireNonNull;

/** Response for {@link JarRunApplicationHandler}. */
public class JarRunApplicationResponseBody implements ResponseBody {

    @JsonProperty("applicationid")
    @JsonDeserialize(using = ApplicationIDDeserializer.class)
    @JsonSerialize(using = ApplicationIDSerializer.class)
    private final ApplicationID applicationId;

    @JsonCreator
    public JarRunApplicationResponseBody(
            @JsonProperty("applicationid") @JsonDeserialize(using = ApplicationIDDeserializer.class)
                    final ApplicationID applicationId) {
        this.applicationId = requireNonNull(applicationId);
    }

    public ApplicationID getApplicationId() {
        return applicationId;
    }
}
