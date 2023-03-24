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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to execute a statement. */
public class ExecuteStatementHandler
        extends AbstractSqlGatewayRestHandler<
                ExecuteStatementRequestBody,
                ExecuteStatementResponseBody,
                SessionMessageParameters> {

    public ExecuteStatementHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            ExecuteStatementRequestBody,
                            ExecuteStatementResponseBody,
                            SessionMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<ExecuteStatementResponseBody> handleRequest(
            SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<ExecuteStatementRequestBody> request) {
        String statement = request.getRequestBody().getStatement();
        Long timeout = request.getRequestBody().getTimeout();
        timeout = timeout == null ? 0L : timeout;
        SessionHandle sessionHandle = request.getPathParameter(SessionHandleIdPathParameter.class);
        Map<String, String> executionConfigMap = request.getRequestBody().getExecutionConfig();
        Configuration executionConfig =
                executionConfigMap == null
                        ? new Configuration()
                        : Configuration.fromMap(executionConfigMap);

        OperationHandle operationHandle =
                service.executeStatement(sessionHandle, statement, timeout, executionConfig);

        return CompletableFuture.completedFuture(
                new ExecuteStatementResponseBody(operationHandle.getIdentifier().toString()));
    }
}
