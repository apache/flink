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

package org.apache.flink.table.gateway.rest.header.session;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.session.CloseSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for closing a session. */
public class CloseSessionHeaders
        implements SqlGatewayMessageHeaders<
                EmptyRequestBody, CloseSessionResponseBody, SessionMessageParameters> {

    private static final CloseSessionHeaders INSTANCE = new CloseSessionHeaders();

    private static final String URL = "/sessions/:" + SessionHandleIdPathParameter.KEY;

    private CloseSessionHeaders() {}

    @Override
    public Class<CloseSessionResponseBody> getResponseClass() {
        return CloseSessionResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Closes the specific session.";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public SessionMessageParameters getUnresolvedMessageParameters() {
        return new SessionMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.DELETE;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static CloseSessionHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String operationId() {
        return "closeSession";
    }
}
