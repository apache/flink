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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractExecutionGraphHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.rescales.JobIDRescaleIDParameters;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetails;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleIDPathParameter;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescaleIdInfo;
import org.apache.flink.runtime.scheduler.adaptive.timeline.RescalesStatsSnapshot;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/** Handler serving the job rescale. */
public class JobRescaleDetailsHandler
        extends AbstractExecutionGraphHandler<JobRescaleDetails, JobIDRescaleIDParameters>
        implements JsonArchivist {

    public JobRescaleDetailsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobRescaleDetails, JobIDRescaleIDParameters>
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
    protected JobRescaleDetails handleRequest(
            HandlerRequest<EmptyRequestBody> request, ExecutionGraphInfo executionGraphInfo)
            throws RestHandlerException {

        RescalesStatsSnapshot rescalesStatsSnapshot = executionGraphInfo.getRescalesStatsSnapshot();
        JobID jobId = executionGraphInfo.getJobId();

        if (rescalesStatsSnapshot == null) {
            throw new RestHandlerException(
                    "AdaptiveScheduler rescales was not enabled for job " + jobId + '.',
                    HttpResponseStatus.NOT_FOUND);
        }

        RescaleIdInfo.RescaleUUID rescaleUuid =
                request.getPathParameter(JobRescaleIDPathParameter.class);
        Rescale rescale = rescalesStatsSnapshot.getRescale(rescaleUuid);

        if (rescale == null) {
            throw new RestHandlerException(
                    "Could not find rescale details of specified rescaleUuid " + rescaleUuid + '.',
                    HttpResponseStatus.NOT_FOUND);
        }

        return JobRescaleDetails.fromRescale(rescale, true);
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(ExecutionGraphInfo executionGraphInfo)
            throws IOException {
        if (executionGraphInfo.getRescalesStatsSnapshot() == null
                || executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory() == null) {
            return List.of();
        }

        List<ArchivedJson> archives = new ArrayList<>();
        List<Rescale> rescales = executionGraphInfo.getRescalesStatsSnapshot().getRescaleHistory();
        final String commonPathPrefix =
                JobRescaleDetailsHeaders.getInstance()
                        .getTargetRestEndpointURL()
                        .replace(
                                ':' + JobIDPathParameter.KEY,
                                executionGraphInfo.getJobId().toString());
        for (Rescale rescale : rescales) {
            archives.add(
                    new ArchivedJson(
                            commonPathPrefix.replace(
                                    ':' + JobRescaleIDPathParameter.KEY,
                                    rescale.getRescaleIdInfo().getRescaleUuid().toString()),
                            JobRescaleDetails.fromRescale(rescale, true)));
        }
        return archives;
    }
}
