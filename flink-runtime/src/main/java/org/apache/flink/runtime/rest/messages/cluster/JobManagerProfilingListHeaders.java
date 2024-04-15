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

package org.apache.flink.runtime.rest.messages.cluster;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.cluster.JobManagerProfilingHandler;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ProfilingInfoList;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/** Headers for the {@link JobManagerProfilingHandler}. */
public class JobManagerProfilingListHeaders
        implements MessageHeaders<EmptyRequestBody, ProfilingInfoList, EmptyMessageParameters> {

    private static final JobManagerProfilingListHeaders INSTANCE =
            new JobManagerProfilingListHeaders();

    private static final String URL = "/jobmanager/profiler";

    private JobManagerProfilingListHeaders() {}

    public static JobManagerProfilingListHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<ProfilingInfoList> getResponseClass() {
        return ProfilingInfoList.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Returns the profiling history on JobManager.";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return Collections.singleton(RuntimeRestAPIVersion.V1);
    }
}
