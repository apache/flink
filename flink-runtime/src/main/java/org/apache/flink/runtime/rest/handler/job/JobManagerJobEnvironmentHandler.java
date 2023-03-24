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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EnvironmentInfo;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.JobManagerJobEnvironmentHeaders;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler which serves the jobmanager's environment variables of a specific job. */
public class JobManagerJobEnvironmentHandler
        extends AbstractRestHandler<
                RestfulGateway, EmptyRequestBody, EnvironmentInfo, JobMessageParameters>
        implements JsonArchivist {
    private final EnvironmentInfo environmentInfo;

    public JobManagerJobEnvironmentHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, EnvironmentInfo, JobMessageParameters>
                    messageHeaders) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);

        this.environmentInfo = EnvironmentInfo.create();
    }

    @Override
    protected CompletableFuture<EnvironmentInfo> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        return CompletableFuture.completedFuture(environmentInfo);
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(ExecutionGraphInfo executionGraphInfo)
            throws IOException {
        return Collections.singletonList(
                new ArchivedJson(
                        JobManagerJobEnvironmentHeaders.getInstance()
                                .getTargetRestEndpointURL()
                                .replace(
                                        ':' + JobIDPathParameter.KEY,
                                        executionGraphInfo.getJobId().toString()),
                        environmentInfo));
    }
}
