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
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to open session. */
public class OpenSessionHandler
        extends AbstractSqlGatewayRestHandler<
                OpenSessionRequestBody, OpenSessionResponseBody, EmptyMessageParameters> {

    public OpenSessionHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<OpenSessionRequestBody, OpenSessionResponseBody, EmptyMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<OpenSessionResponseBody> handleRequest(
            SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<OpenSessionRequestBody> request) {
        String sessionName = request.getRequestBody().getSessionName();
        Map<String, String> properties = request.getRequestBody().getProperties();
        SessionHandle sessionHandle =
                service.openSession(
                        SessionEnvironment.newBuilder()
                                .setSessionEndpointVersion(version)
                                .setSessionName(sessionName)
                                .addSessionConfig(properties)
                                .build());

        return CompletableFuture.completedFuture(
                new OpenSessionResponseBody(sessionHandle.getIdentifier().toString()));
    }
}
