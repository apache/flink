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

package org.apache.flink.table.gateway.rest.header.application;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.application.DeployScriptRequestBody;
import org.apache.flink.table.gateway.rest.message.application.DeployScriptResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;

/** Message headers for run the script in application mode. */
public class DeployScriptHeaders
        implements SqlGatewayMessageHeaders<
                DeployScriptRequestBody, DeployScriptResponseBody, SessionMessageParameters> {

    private static final DeployScriptHeaders INSTANCE = new DeployScriptHeaders();

    private static final String URL = "/sessions/:" + SessionHandleIdPathParameter.KEY + "/scripts";

    public static DeployScriptHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public Class<DeployScriptResponseBody> getResponseClass() {
        return DeployScriptResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Deploy the script in application mode";
    }

    @Override
    public Class<DeployScriptRequestBody> getRequestClass() {
        return DeployScriptRequestBody.class;
    }

    @Override
    public SessionMessageParameters getUnresolvedMessageParameters() {
        return new SessionMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return SqlGatewayRestAPIVersion.getHigherVersions(SqlGatewayRestAPIVersion.V3);
    }

    @Override
    public String operationId() {
        return "deployScript";
    }
}
