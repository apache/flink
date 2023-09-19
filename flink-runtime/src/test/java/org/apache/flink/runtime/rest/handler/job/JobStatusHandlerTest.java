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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.messages.webmonitor.JobStatusInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobStatusInfoHeaders;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link JobStatusHandler}. */
class JobStatusHandlerTest {
    @Test
    void testRequestJobStatus() throws Exception {
        final JobStatusHandler jobStatusHandler =
                new JobStatusHandler(
                        CompletableFuture::new,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobStatusInfoHeaders.getInstance());

        final HandlerRequest<EmptyRequestBody> request = createRequest(new JobID());
        final CompletableFuture<JobStatusInfo> response =
                jobStatusHandler.handleRequest(
                        request,
                        new TestingRestfulGateway.Builder()
                                .setRequestJobStatusFunction(
                                        ignored ->
                                                CompletableFuture.completedFuture(
                                                        JobStatus.INITIALIZING))
                                .build());

        assertThat(JobStatus.INITIALIZING).isEqualTo(response.get().getJobStatus());
    }

    private static HandlerRequest<EmptyRequestBody> createRequest(JobID jobId)
            throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toString());

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new JobMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
