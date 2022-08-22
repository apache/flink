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
import org.apache.flink.runtime.rest.messages.ConfigurationInfo;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for the {@link
 * org.apache.flink.runtime.rest.handler.job.JobManagerJobConfigurationHandler}.
 */
public class JobManagerJobConfigurationHeaders
        implements RuntimeMessageHeaders<
                EmptyRequestBody, ConfigurationInfo, JobMessageParameters> {
    private static final JobManagerJobConfigurationHeaders INSTANCE =
            new JobManagerJobConfigurationHeaders();

    public static final String JOBMANAGER_JOB_CONFIG_REST_PATH = "/jobs/:jobid/jobmanager/config";

    private JobManagerJobConfigurationHeaders() {}

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
        return JOBMANAGER_JOB_CONFIG_REST_PATH;
    }

    @Override
    public Class<ConfigurationInfo> getResponseClass() {
        return ConfigurationInfo.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public JobMessageParameters getUnresolvedMessageParameters() {
        return new JobMessageParameters();
    }

    public static JobManagerJobConfigurationHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Returns the jobmanager's configuration of a specific job.";
    }
}
