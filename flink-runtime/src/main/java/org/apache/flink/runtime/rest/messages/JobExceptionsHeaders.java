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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for the {@link JobExceptionsHandler}. */
public class JobExceptionsHeaders
        implements MessageHeaders<
                EmptyRequestBody, JobExceptionsInfoWithHistory, JobExceptionsMessageParameters> {

    private static final JobExceptionsHeaders INSTANCE = new JobExceptionsHeaders();

    public static final String URL = "/jobs/:jobid/exceptions";

    private JobExceptionsHeaders() {}

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public Class<JobExceptionsInfoWithHistory> getResponseClass() {
        return JobExceptionsInfoWithHistory.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public JobExceptionsMessageParameters getUnresolvedMessageParameters() {
        return new JobExceptionsMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static JobExceptionsHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return String.format(
                "Returns the most recent exceptions that have been handled by Flink for this job. The "
                        + "'exceptionHistory.truncated' flag defines whether exceptions were filtered "
                        + "out through the GET parameter. The backend collects only a specific amount "
                        + "of most recent exceptions per job. This can be configured through %s in the "
                        + "Flink configuration. The following first-level members are deprecated: "
                        + "'root-exception', 'timestamp', 'all-exceptions', and 'truncated'. Use the data provided "
                        + "through 'exceptionHistory', instead.",
                WebOptions.MAX_EXCEPTION_HISTORY_SIZE.key());
    }
}
