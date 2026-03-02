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

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
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
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesOverview;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescalesOverviewHeaders;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/** Handler to respond with a job rescales overview. */
public class JobRescalesOverviewHandler
        extends AbstractExecutionGraphHandler<JobRescalesOverview, JobMessageParameters>
        implements JsonArchivist {

    public JobRescalesOverviewHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobRescalesOverview, JobMessageParameters>
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
    protected JobRescalesOverview handleRequest(
            HandlerRequest<EmptyRequestBody> request, ExecutionGraphInfo executionGraphInfo)
            throws RestHandlerException {
        return getJobRescalesOverview(executionGraphInfo);
    }

    private JobRescalesOverview getJobRescalesOverview(ExecutionGraphInfo executionGraphInfo)
            throws RestHandlerException {
        RescalesStatsSnapshot rescalesStatsSnapshot = executionGraphInfo.getRescalesStatsSnapshot();
        if (rescalesStatsSnapshot == null || rescalesStatsSnapshot.getRescaleHistory() == null) {
            throw new RestHandlerException(
                    String.format(
                            "The job `%s` has not enabled the `%s` scheduler, or it has been enabled but the value of configuration option `%s` has not been set to greater than `0`.",
                            executionGraphInfo.getJobId(),
                            JobManagerOptions.SchedulerType.Adaptive,
                            WebOptions.MAX_ADAPTIVE_SCHEDULER_RESCALE_HISTORY_SIZE.key()),
                    HttpResponseStatus.NOT_FOUND,
                    RestHandlerException.LoggingBehavior.IGNORE);
        }
        return JobRescalesOverview.fromRescalesStatsSnapshot(
                executionGraphInfo.getRescalesStatsSnapshot());
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(ExecutionGraphInfo executionGraphInfo)
            throws IOException {

        ResponseBody response;
        try {
            response = getJobRescalesOverview(executionGraphInfo);
        } catch (RestHandlerException rhe) {
            response = new ErrorResponseBody(rhe.getMessage());
        }
        return List.of(
                new ArchivedJson(
                        JobRescalesOverviewHeaders.getInstance()
                                .getTargetRestEndpointURL()
                                .replace(
                                        ':' + JobIDPathParameter.KEY,
                                        executionGraphInfo.getJobId().toString()),
                        response));
    }
}
