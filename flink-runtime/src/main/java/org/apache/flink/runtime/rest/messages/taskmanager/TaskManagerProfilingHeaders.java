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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerProfilingHandler;
import org.apache.flink.runtime.rest.messages.ProfilingInfo;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.messages.cluster.ProfilingRequestBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Headers for the {@link TaskManagerProfilingHandler}. */
public class TaskManagerProfilingHeaders
        implements RuntimeMessageHeaders<
                ProfilingRequestBody, ProfilingInfo, TaskManagerMessageParameters> {

    private static final TaskManagerProfilingHeaders INSTANCE = new TaskManagerProfilingHeaders();

    private static final String URL =
            String.format("/taskmanagers/:%s/profiler", TaskManagerIdPathParameter.KEY);

    private TaskManagerProfilingHeaders() {}

    @Override
    public Class<ProfilingRequestBody> getRequestClass() {
        return ProfilingRequestBody.class;
    }

    @Override
    public TaskManagerMessageParameters getUnresolvedMessageParameters() {
        return new TaskManagerMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static TaskManagerProfilingHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<ProfilingInfo> getResponseClass() {
        return ProfilingInfo.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Returns the profiling instance of the requested TaskManager.";
    }
}
