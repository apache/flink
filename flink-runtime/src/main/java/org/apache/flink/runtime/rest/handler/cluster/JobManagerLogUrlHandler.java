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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobManagerLogUrlHeaders;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.LogUrlResponse;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.EnvironmentInfoUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Request handler for retrieving the job manager log url. */
@Internal
public class JobManagerLogUrlHandler
        extends AbstractRestHandler<
                RestfulGateway, EmptyRequestBody, LogUrlResponse, JobMessageParameters>
        implements JsonArchivist {
    private final EnvironmentInfoUtils.EnvironmentContext environmentContext;

    public JobManagerLogUrlHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, LogUrlResponse, JobMessageParameters> messageHeaders,
            EnvironmentInfoUtils.EnvironmentContext environmentContext) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.environmentContext = Preconditions.checkNotNull(environmentContext);
    }

    @Override
    protected CompletableFuture<LogUrlResponse> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        return CompletableFuture.completedFuture(createJobManagerUrl(environmentContext));
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(ExecutionGraphInfo executionGraphInfo)
            throws IOException {
        ResponseBody json = createJobManagerUrl(environmentContext);
        String path =
                JobManagerLogUrlHeaders.getInstance()
                        .getTargetRestEndpointURL()
                        .replace(
                                ':' + JobIDPathParameter.KEY,
                                executionGraphInfo.getJobId().toString());
        return Collections.singletonList(new ArchivedJson(path, json));
    }

    /**
     * Generates a URL that will link to the location of the job manager logs. The format of the URL
     * will be: http://NM-HOST:NM-HTTP-PORT/node/containerlogs/CONTAINER-ID/USER
     */
    @VisibleForTesting
    static LogUrlResponse createJobManagerUrl(
            EnvironmentInfoUtils.EnvironmentContext environmentContext) {
        return new LogUrlResponse(
                EnvironmentInfoUtils.createNodeManagerLogUrl(
                        environmentContext.nodeManagerHostName,
                        environmentContext.nodeManagerHttpPort,
                        environmentContext.containerId,
                        environmentContext.user));
    }
}
