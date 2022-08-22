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

package org.apache.flink.table.gateway.rest.handler.session;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.session.CloseSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to close session. */
public class CloseSessionHandler
        extends AbstractSqlGatewayRestHandler<
                EmptyRequestBody, CloseSessionResponseBody, SessionMessageParameters> {

    public static final String CLOSE_MESSAGE = "CLOSED";

    public CloseSessionHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, CloseSessionResponseBody, SessionMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<CloseSessionResponseBody> handleRequest(
            SqlGatewayRestAPIVersion version, @Nonnull HandlerRequest<EmptyRequestBody> request)
            throws RestHandlerException {
        try {
            SessionHandle sessionHandle =
                    request.getPathParameter(SessionHandleIdPathParameter.class);
            this.service.closeSession(sessionHandle);
            return CompletableFuture.completedFuture(new CloseSessionResponseBody(CLOSE_MESSAGE));
        } catch (SqlGatewayException e) {
            throw new RestHandlerException(
                    e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}
