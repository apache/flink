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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.LogUrlResponse;
import org.apache.flink.runtime.rest.util.EnvironmentInfoUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link JobManagerLogUrlHandler}. */
class JobManagerLogUrlHandlerTest {

    private static final String EXPECTED_URL =
            "http://foo.bar:8042/node/containerlogs/container_abc/user";

    private final JobManagerLogUrlHandler testInstance =
            new JobManagerLogUrlHandler(
                    () -> null,
                    TestingUtils.TIMEOUT,
                    Collections.emptyMap(),
                    JobManagerLogUrlHeaders.getInstance(),
                    new EnvironmentInfoUtils.EnvironmentContext(
                            "container_abc", "foo.bar", "8042", "user"));

    @Test
    void testGenerateJobManagerLogUrl() {
        final LogUrlResponse actual =
                JobManagerLogUrlHandler.createJobManagerUrl(
                        new EnvironmentInfoUtils.EnvironmentContext(
                                "container_abc", "foo.bar", "8042", "user"));

        assertThat(actual).isEqualTo(new LogUrlResponse(EXPECTED_URL));
    }

    @Test
    void testHandleRequestReturnsLogUrl() throws Exception {
        final JobMessageParameters parameters = new JobMessageParameters();
        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        parameters,
                        Collections.singletonMap(JobIDPathParameter.KEY, new JobID().toString()),
                        Collections.emptyMap(),
                        Collections.emptyList());

        final LogUrlResponse response =
                (LogUrlResponse)
                        testInstance
                                .handleRequest(request, new TestingRestfulGateway.Builder().build())
                                .get();

        assertThat(response).isEqualTo(new LogUrlResponse(EXPECTED_URL));
    }

    @Test
    void testArchivedJsonUsesJobIdInPath() throws Exception {
        final JobID jobId = new JobID();

        final Collection<ArchivedJson> archived =
                testInstance.archiveJsonWithPath(
                        new ExecutionGraphInfo(
                                new ArchivedExecutionGraphBuilder().setJobID(jobId).build()));

        assertThat(archived).hasSize(1);
        final ArchivedJson json = archived.iterator().next();
        assertThat(json.getPath()).isEqualTo("/jobs/" + jobId + "/jobmanager/log-url");
        assertThat(json.getJson()).contains(EXPECTED_URL);
    }
}
