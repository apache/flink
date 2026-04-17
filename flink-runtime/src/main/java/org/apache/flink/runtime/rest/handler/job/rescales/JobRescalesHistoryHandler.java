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

package org.apache.flink.runtime.rest.handler.job.rescales;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractExecutionGraphHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesHistory;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesHistoryHeaders;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

/** Handler to response job rescales history. */
public class JobRescalesHistoryHandler
        extends AbstractExecutionGraphHandler<JobRescalesHistory, JobMessageParameters>
        implements JsonArchivist {

    public JobRescalesHistoryHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobRescalesHistory, JobMessageParameters>
                    messageHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executionGraphCache,
                executor);
    }

    @Override
    protected JobRescalesHistory handleRequest(
            HandlerRequest<EmptyRequestBody> request, ExecutionGraphInfo executionGraphInfo)
            throws RestHandlerException {
        return getJobRescalesHistory(executionGraphInfo);
    }

    private JobRescalesHistory getJobRescalesHistory(ExecutionGraphInfo executionGraphInfo)
            throws RestHandlerException {
        if (executionGraphInfo.getRescalesStatsSnapshot() == null
                || executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory() == null) {
            throw RescalesUnavailableException.createForJob(executionGraphInfo.getJobId());
        }
        return JobRescalesHistory.fromRescalesStatsSnapshot(
                executionGraphInfo.getRescalesStatsSnapshot());
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(ExecutionGraphInfo executionGraphInfo)
            throws IOException {

        ResponseBody response;
        try {
            response = getJobRescalesHistory(executionGraphInfo);
        } catch (RestHandlerException rhe) {
            response = new ErrorResponseBody(rhe.getMessage());
        }
        return Collections.singletonList(
                new ArchivedJson(
                        JobRescalesHistoryHeaders.getInstance()
                                .getTargetRestEndpointURL()
                                .replace(
                                        ':' + JobIDPathParameter.KEY,
                                        executionGraphInfo.getJobId().toString()),
                        response));
    }
}
