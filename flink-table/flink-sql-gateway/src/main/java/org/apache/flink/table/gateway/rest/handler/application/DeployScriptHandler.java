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

package org.apache.flink.table.gateway.rest.handler.application;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.application.DeployScriptRequestBody;
import org.apache.flink.table.gateway.rest.message.application.DeployScriptResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to deploy the script in application mode. */
public class DeployScriptHandler
        extends AbstractSqlGatewayRestHandler<
                DeployScriptRequestBody, DeployScriptResponseBody, SessionMessageParameters> {

    public DeployScriptHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            DeployScriptRequestBody,
                            DeployScriptResponseBody,
                            SessionMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<DeployScriptResponseBody> handleRequest(
            @Nullable SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<DeployScriptRequestBody> request)
            throws RestHandlerException {
        return CompletableFuture.completedFuture(
                new DeployScriptResponseBody(
                        service.deployScript(
                                        request.getPathParameter(
                                                SessionHandleIdPathParameter.class),
                                        request.getRequestBody().getScriptUri() == null
                                                ? null
                                                : URI.create(
                                                        request.getRequestBody().getScriptUri()),
                                        request.getRequestBody().getScript(),
                                        Configuration.fromMap(
                                                request.getRequestBody().getExecutionConfig()))
                                .toString()));
    }
}
