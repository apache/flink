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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDumpInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointDumpMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Request handler for pending savepoints used in FFS. */
public class SavepointDumpHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                SavepointDumpInfo,
                SavepointDumpMessageParameters> {

    public SavepointDumpHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, SavepointDumpInfo, SavepointDumpMessageParameters>
                    messageHeaders) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<SavepointDumpInfo> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        List<String> pendingSavepointIds = new ArrayList<>();
        final JobID jobID = request.getPathParameter(JobIDPathParameter.class);
        try {
            pendingSavepointIds.addAll(
                    gateway.dumpPendingSavepoints(jobID)
                            .get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            log.warn("Timeout for getting all pending savepoints.");
        }
        return CompletableFuture.completedFuture(new SavepointDumpInfo(pendingSavepointIds));
    }
}
