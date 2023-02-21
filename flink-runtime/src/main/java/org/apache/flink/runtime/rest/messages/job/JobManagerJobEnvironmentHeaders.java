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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EnvironmentInfo;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for the {@link
 * org.apache.flink.runtime.rest.handler.job.JobManagerJobEnvironmentHandler}.
 */
public class JobManagerJobEnvironmentHeaders
        implements RuntimeMessageHeaders<EmptyRequestBody, EnvironmentInfo, JobMessageParameters> {
    private static final JobManagerJobEnvironmentHeaders INSTANCE =
            new JobManagerJobEnvironmentHeaders();

    public static final String JOBMANAGER_JOB_ENV_REST_PATH = "/jobs/:jobid/jobmanager/environment";

    private JobManagerJobEnvironmentHeaders() {}

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return JOBMANAGER_JOB_ENV_REST_PATH;
    }

    @Override
    public Class<EnvironmentInfo> getResponseClass() {
        return EnvironmentInfo.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public JobMessageParameters getUnresolvedMessageParameters() {
        return new JobMessageParameters();
    }

    public static JobManagerJobEnvironmentHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Returns the jobmanager's environment of a specific job.";
    }
}
