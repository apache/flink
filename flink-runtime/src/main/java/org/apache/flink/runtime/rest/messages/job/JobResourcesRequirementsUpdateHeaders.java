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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Headers for REST request to patch a job. */
public class JobResourcesRequirementsUpdateHeaders
        implements RuntimeMessageHeaders<
                JobResourceRequirementsBody, EmptyResponseBody, JobMessageParameters> {

    public static final JobResourcesRequirementsUpdateHeaders INSTANCE =
            new JobResourcesRequirementsUpdateHeaders();

    private static final String URL = "/jobs/:" + JobIDPathParameter.KEY + "/resource-requirements";

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.PUT;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<EmptyResponseBody> getResponseClass() {
        return EmptyResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Request to update job's resource requirements.";
    }

    @Override
    public Class<JobResourceRequirementsBody> getRequestClass() {
        return JobResourceRequirementsBody.class;
    }

    @Override
    public JobMessageParameters getUnresolvedMessageParameters() {
        return new JobMessageParameters();
    }

    @Override
    public String operationId() {
        return "updateJobResourceRequirements";
    }
}
