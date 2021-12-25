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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Tests for the {@link JobDetails}. */
public class JobDetailsTest extends TestLogger {
    private static final String COMPATIBLE_JOB_DETAILS =
            "{"
                    + "  \"jid\" : \"7a7c3291accebd10b6be8d4f8c8d8dfc\","
                    + "  \"name\" : \"foobar\","
                    + "  \"state\" : \"RUNNING\","
                    + "  \"start-time\" : 1,"
                    + "  \"end-time\" : 10,"
                    + "  \"duration\" : 9,"
                    + "  \"last-modification\" : 8,"
                    + "  \"tasks\" : {"
                    + "    \"total\" : 42,"
                    + "    \"created\" : 1,"
                    + "    \"scheduled\" : 3,"
                    + "    \"deploying\" : 3,"
                    + "    \"running\" : 4,"
                    + "    \"finished\" : 7,"
                    + "    \"canceling\" : 4,"
                    + "    \"canceled\" : 2,"
                    + "    \"failed\" : 7,"
                    + "    \"reconciling\" : 3"
                    + "  }"
                    + "}";

    /** Tests that we can marshal and unmarshal JobDetails instances. */
    @Test
    public void testJobDetailsMarshalling() throws JsonProcessingException {
        final JobDetails expected =
                new JobDetails(
                        new JobID(),
                        "foobar",
                        1L,
                        10L,
                        9L,
                        JobStatus.RUNNING,
                        8L,
                        new int[] {1, 3, 3, 4, 7, 4, 2, 7, 3, 3},
                        42);

        final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

        final JsonNode marshalled = objectMapper.valueToTree(expected);

        final JobDetails unmarshalled = objectMapper.treeToValue(marshalled, JobDetails.class);

        assertEquals(expected, unmarshalled);
    }

    @Test
    public void testJobDetailsCompatibleUnmarshalling() throws IOException {
        final JobDetails expected =
                new JobDetails(
                        JobID.fromHexString("7a7c3291accebd10b6be8d4f8c8d8dfc"),
                        "foobar",
                        1L,
                        10L,
                        9L,
                        JobStatus.RUNNING,
                        8L,
                        new int[] {1, 3, 3, 4, 7, 4, 2, 7, 3, 0},
                        42);

        final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

        final JobDetails unmarshalled =
                objectMapper.readValue(COMPATIBLE_JOB_DETAILS, JobDetails.class);

        assertEquals(expected, unmarshalled);
    }
}
