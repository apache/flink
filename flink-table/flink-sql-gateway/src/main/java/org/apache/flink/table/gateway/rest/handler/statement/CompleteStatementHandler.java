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

package org.apache.flink.table.gateway.rest.handler.statement;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.CompleteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.CompleteStatementResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to complete a statement. */
public class CompleteStatementHandler
        extends AbstractSqlGatewayRestHandler<
                CompleteStatementRequestBody,
                CompleteStatementResponseBody,
                SessionMessageParameters> {

    public CompleteStatementHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            CompleteStatementRequestBody,
                            CompleteStatementResponseBody,
                            SessionMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<CompleteStatementResponseBody> handleRequest(
            SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<CompleteStatementRequestBody> request) {
        SessionHandle sessionHandle = request.getPathParameter(SessionHandleIdPathParameter.class);
        String statement = request.getRequestBody().getStatement();
        int position = request.getRequestBody().getPosition();

        return CompletableFuture.completedFuture(
                new CompleteStatementResponseBody(
                        service.completeStatement(sessionHandle, statement, position)));
    }
}
