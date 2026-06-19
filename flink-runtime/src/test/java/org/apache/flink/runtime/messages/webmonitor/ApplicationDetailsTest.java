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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ApplicationDetails}. */
class ApplicationDetailsTest {

    private static final String COMPATIBLE_APPLICATION_DETAILS =
            "{"
                    + "  \"id\" : \"7a7c3291accebd10b6be8d4f8c8d8dfc\","
                    + "  \"name\" : \"foobar\","
                    + "  \"status\" : \"RUNNING\","
                    + "  \"start-time\" : 1,"
                    + "  \"end-time\" : 10,"
                    + "  \"duration\" : 9,"
                    + "  \"jobs\" : {"
                    + "    \"RUNNING\" : 42,"
                    + "    \"FINISHED\" : 1"
                    + "  }"
                    + "}";
    private static final String UNKNOWN_FIELD_APPLICATION_DETAILS =
            "{"
                    + "  \"id\" : \"7a7c3291accebd10b6be8d4f8c8d8dfc\","
                    + "  \"intentionally_unknown_which_must_be_skipped\" : 0,"
                    + "  \"name\" : \"foobar\","
                    + "  \"status\" : \"RUNNING\","
                    + "  \"start-time\" : 1,"
                    + "  \"end-time\" : 10,"
                    + "  \"duration\" : 9,"
                    + "  \"jobs\" : {"
                    + "    \"RUNNING\" : 42,"
                    + "    \"FINISHED\" : 1"
                    + "  }"
                    + "}";

    private ObjectMapper objectMapper;
    private ObjectMapper flexibleObjectMapper;

    final ApplicationDetails expected =
            new ApplicationDetails(
                    ApplicationID.fromHexString("7a7c3291accebd10b6be8d4f8c8d8dfc"),
                    "foobar",
                    1L,
                    10L,
                    9L,
                    ApplicationState.RUNNING.name(),
                    mockJobInfo());

    @BeforeEach
    public void beforeEach() {
        objectMapper = RestMapperUtils.getStrictObjectMapper();
        flexibleObjectMapper = RestMapperUtils.getFlexibleObjectMapper();
    }

    /** Tests that we can marshal and unmarshal JobDetails instances. */
    @Test
    void testJobDetailsMarshalling() throws JsonProcessingException {
        final JsonNode marshalled = objectMapper.valueToTree(expected);

        final ApplicationDetails unmarshalled =
                objectMapper.treeToValue(marshalled, ApplicationDetails.class);

        assertThat(unmarshalled).isEqualTo(expected);
    }

    @Test
    void testJobDetailsCompatibleUnmarshalling() throws IOException {
        final ApplicationDetails unmarshalled =
                objectMapper.readValue(COMPATIBLE_APPLICATION_DETAILS, ApplicationDetails.class);

        assertThat(unmarshalled).isEqualTo(expected);
    }

    @Test
    void testJobDetailsCompatibleUnmarshallingSkipUnknown() throws IOException {
        final ApplicationDetails unmarshalled =
                flexibleObjectMapper.readValue(
                        UNKNOWN_FIELD_APPLICATION_DETAILS, ApplicationDetails.class);

        assertThat(unmarshalled).isEqualTo(expected);
    }

    static Map<String, Integer> mockJobInfo() {
        Map<String, Integer> jobInfo = new HashMap<>();
        jobInfo.put(JobStatus.RUNNING.name(), 42);
        jobInfo.put(JobStatus.FINISHED.name(), 1);
        return jobInfo;
    }
}
