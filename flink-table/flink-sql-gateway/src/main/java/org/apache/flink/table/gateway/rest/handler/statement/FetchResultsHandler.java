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
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.HandlerRequestUtils;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.operation.OperationHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultResponseBodyImpl;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsRowFormatQueryParameter;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsTokenPathParameter;
import org.apache.flink.table.gateway.rest.message.statement.NotReadyFetchResultResponse;
import org.apache.flink.table.gateway.rest.serde.ResultInfo;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to fetch results. */
public class FetchResultsHandler
        extends AbstractSqlGatewayRestHandler<
                EmptyRequestBody, FetchResultsResponseBody, FetchResultsMessageParameters> {

    public FetchResultsHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            EmptyRequestBody,
                            FetchResultsResponseBody,
                            FetchResultsMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<FetchResultsResponseBody> handleRequest(
            SqlGatewayRestAPIVersion version, @Nonnull HandlerRequest<EmptyRequestBody> request)
            throws RestHandlerException {
        // Parse the parameters
        SessionHandle sessionHandle = request.getPathParameter(SessionHandleIdPathParameter.class);
        OperationHandle operationHandle =
                request.getPathParameter(OperationHandleIdPathParameter.class);
        Long token = request.getPathParameter(FetchResultsTokenPathParameter.class);
        RowFormat rowFormat =
                HandlerRequestUtils.getQueryParameter(
                        request, FetchResultsRowFormatQueryParameter.class, RowFormat.JSON);

        // Get the statement results
        ResultSet resultSet;
        try {
            resultSet =
                    service.fetchResults(sessionHandle, operationHandle, token, Integer.MAX_VALUE);
        } catch (Exception e) {
            throw new SqlGatewayException(e);
        }

        ResultSet.ResultType resultType = resultSet.getResultType();
        Long nextToken = resultSet.getNextToken();
        String nextResultUri =
                FetchResultsHeaders.buildNextUri(
                        version,
                        sessionHandle.getIdentifier().toString(),
                        operationHandle.getIdentifier().toString(),
                        nextToken,
                        rowFormat);

        // Build the response
        if (resultType == ResultSet.ResultType.NOT_READY) {
            return CompletableFuture.completedFuture(
                    new NotReadyFetchResultResponse(nextResultUri));
        } else {
            return CompletableFuture.completedFuture(
                    new FetchResultResponseBodyImpl(
                            resultType,
                            resultSet.isQueryResult(),
                            resultSet.getJobID(),
                            resultSet.getResultKind(),
                            ResultInfo.createResultInfo(resultSet, rowFormat),
                            nextResultUri));
        }
    }
}
