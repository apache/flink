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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.job.JobVertexFlameGraphHandler;
import org.apache.flink.runtime.webmonitor.threadinfo.JobVertexFlameGraph;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for the {@link JobVertexFlameGraphHandler}. */
public class JobVertexFlameGraphHeaders
        implements MessageHeaders<
                EmptyRequestBody, JobVertexFlameGraph, JobVertexFlameGraphParameters> {

    private static final JobVertexFlameGraphHeaders INSTANCE = new JobVertexFlameGraphHeaders();

    private static final String URL =
            "/jobs/:"
                    + JobIDPathParameter.KEY
                    + "/vertices/:"
                    + JobVertexIdPathParameter.KEY
                    + "/flamegraph";

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public Class<JobVertexFlameGraph> getResponseClass() {
        return JobVertexFlameGraph.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public JobVertexFlameGraphParameters getUnresolvedMessageParameters() {
        return new JobVertexFlameGraphParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static JobVertexFlameGraphHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Returns flame graph information for a vertex, and may initiate flame graph sampling if necessary.";
    }
}
