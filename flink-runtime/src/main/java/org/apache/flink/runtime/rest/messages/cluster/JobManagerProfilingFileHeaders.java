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
import org.apache.flink.runtime.rest.handler.cluster.JobManagerProfilingFileHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ProfilingFileNamePathParameter;
import org.apache.flink.runtime.rest.messages.RuntimeUntypedResponseMessageHeaders;

/** Headers for the {@link JobManagerProfilingFileHandler}. */
public class JobManagerProfilingFileHeaders
        implements RuntimeUntypedResponseMessageHeaders<
                EmptyRequestBody, ProfilingFileMessageParameters> {

    private static final JobManagerProfilingFileHeaders INSTANCE =
            new JobManagerProfilingFileHeaders();

    private static final String URL =
            String.format("/jobmanager/profiler/:%s", ProfilingFileNamePathParameter.KEY);

    private JobManagerProfilingFileHeaders() {}

    public static JobManagerProfilingFileHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public ProfilingFileMessageParameters getUnresolvedMessageParameters() {
        return new ProfilingFileMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }
}
