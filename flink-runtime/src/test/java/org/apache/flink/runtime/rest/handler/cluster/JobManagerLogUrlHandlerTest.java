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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.runtime.rest.messages.JobManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.messages.LogUrlResponse;
import org.apache.flink.runtime.rest.util.EnvironmentInfoUtils;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for the {@link org.apache.flink.runtime.rest.handler.cluster.JobManagerLogUrlHandler}. */
public class JobManagerLogUrlHandlerTest {

    private JobManagerLogUrlHandler testInstance;

    private static final String CONTAINER_ID = "container_abc";

    private static final String NM_HOST = "foo.bar";

    private static final String NM_PORT = "0000";

    private static final String USER = "user";

    private Configuration configuration;

    @BeforeEach
    void setup() {
        configuration = new Configuration();
        configuration.set(
                HistoryServerOptions
                        .HISTORY_SERVER_JOBMANAGER_TASKMANAGER_LOG_ENABLE_CUSTOM_HANDLERS,
                true);

        testInstance =
                new JobManagerLogUrlHandler(
                        () -> null,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobManagerLogUrlHeaders.getInstance(),
                        this.configuration);
    }

    @Test
    public void testGenerateJobManagerLogUrl() {
        EnvironmentInfoUtils.EnvironmentContext environmentContext =
                new EnvironmentInfoUtils.EnvironmentContext(CONTAINER_ID, NM_HOST, NM_PORT, USER);

        LogUrlResponse actual = testInstance.createJobManagerURL(environmentContext);

        LogUrlResponse expected =
                new LogUrlResponse(
                        String.format(
                                JobManagerLogUrlHandler.JOB_MANAGER_LOG_URL_FORMAT,
                                environmentContext.nodeManagerHostName,
                                environmentContext.containerId,
                                environmentContext.user));

        assertEquals(actual, expected);
    }
}
