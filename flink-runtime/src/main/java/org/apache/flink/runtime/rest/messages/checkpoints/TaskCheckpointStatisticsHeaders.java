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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.job.checkpoints.TaskCheckpointStatisticDetailsHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Headers for the {@link TaskCheckpointStatisticDetailsHandler}. */
public class TaskCheckpointStatisticsHeaders
        implements MessageHeaders<
                EmptyRequestBody,
                TaskCheckpointStatisticsWithSubtaskDetails,
                TaskCheckpointMessageParameters> {

    private static final TaskCheckpointStatisticsHeaders INSTANCE =
            new TaskCheckpointStatisticsHeaders();

    public static final String URL =
            "/jobs/:jobid/checkpoints/details/:checkpointid/subtasks/:vertexid";

    private TaskCheckpointStatisticsHeaders() {}

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public Class<TaskCheckpointStatisticsWithSubtaskDetails> getResponseClass() {
        return TaskCheckpointStatisticsWithSubtaskDetails.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public TaskCheckpointMessageParameters getUnresolvedMessageParameters() {
        return new TaskCheckpointMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static TaskCheckpointStatisticsHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Returns checkpoint statistics for a task and its subtasks.";
    }
}
