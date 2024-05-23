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

package org.apache.flink.table.gateway.rest.header.materializedtable.scheduler;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.scheduler.CreateEmbeddedSchedulerWorkflowResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion.V3;

/** Message headers for create workflow in embedded scheduler. */
public class CreateEmbeddedSchedulerWorkflowHeaders
        implements SqlGatewayMessageHeaders<
                CreateEmbeddedSchedulerWorkflowRequestBody,
                CreateEmbeddedSchedulerWorkflowResponseBody,
                EmptyMessageParameters> {

    private static final CreateEmbeddedSchedulerWorkflowHeaders INSTANCE =
            new CreateEmbeddedSchedulerWorkflowHeaders();

    public static final String URL = "/workflow/embedded-scheduler/create";

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<CreateEmbeddedSchedulerWorkflowResponseBody> getResponseClass() {
        return CreateEmbeddedSchedulerWorkflowResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Create workflow";
    }

    @Override
    public Class<CreateEmbeddedSchedulerWorkflowRequestBody> getRequestClass() {
        return CreateEmbeddedSchedulerWorkflowRequestBody.class;
    }

    @Override
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return Collections.singleton(V3);
    }

    public static CreateEmbeddedSchedulerWorkflowHeaders getInstance() {
        return INSTANCE;
    }
}
