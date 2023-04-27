/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobResourceRequirementsBody;
import org.apache.flink.runtime.rest.messages.job.JobResourcesRequirementsUpdateHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Rest handler for updating {@link org.apache.flink.runtime.jobgraph.JobResourceRequirements
 * resource requirements} of a given job.
 */
public class JobResourceRequirementsUpdateHandler
        extends AbstractRestHandler<
                RestfulGateway,
                JobResourceRequirementsBody,
                EmptyResponseBody,
                JobMessageParameters> {

    public JobResourceRequirementsUpdateHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                JobResourcesRequirementsUpdateHeaders.INSTANCE);
    }

    @Override
    protected CompletableFuture<EmptyResponseBody> handleRequest(
            @Nonnull HandlerRequest<JobResourceRequirementsBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        final Optional<JobResourceRequirements> maybeJobResourceRequirements =
                request.getRequestBody().asJobResourceRequirements();
        if (maybeJobResourceRequirements.isPresent()) {
            return gateway.updateJobResourceRequirements(jobId, maybeJobResourceRequirements.get())
                    .thenApply(ignored -> EmptyResponseBody.getInstance());
        }
        throw new RestHandlerException(
                "Request body does not specify resource requirements.",
                HttpResponseStatus.BAD_REQUEST);
    }
}
