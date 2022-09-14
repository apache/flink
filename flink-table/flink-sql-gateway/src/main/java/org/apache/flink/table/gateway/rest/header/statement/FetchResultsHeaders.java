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

package org.apache.flink.table.gateway.rest.header.statement;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.operation.OperationHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsTokenParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsTokenPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;

/** Message headers for fetching results. */
public class FetchResultsHeaders
        implements SqlGatewayMessageHeaders<
                EmptyRequestBody, FetchResultsResponseBody, FetchResultsTokenParameters> {

    private static final FetchResultsHeaders INSTANCE = new FetchResultsHeaders();

    public static final String URL =
            "/sessions/:"
                    + SessionHandleIdPathParameter.KEY
                    + "/operations/:"
                    + OperationHandleIdPathParameter.KEY
                    + "/result/:"
                    + FetchResultsTokenPathParameter.KEY;

    @Override
    public Class<FetchResultsResponseBody> getResponseClass() {
        return FetchResultsResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Fetch results of Operation.";
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static FetchResultsHeaders getInstance() {
        return INSTANCE;
    }

    @Nullable
    public static String buildNextUri(
            String version, String sessionId, String operationId, Long nextToken) {
        if (nextToken != null) {
            return String.format(
                    "/%s/sessions/%s/operations/%s/result/%s",
                    version, sessionId, operationId, nextToken);
        } else {
            // Empty uri indicates there is no more data
            return null;
        }
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public FetchResultsTokenParameters getUnresolvedMessageParameters() {
        return new FetchResultsTokenParameters();
    }

    @Override
    public String operationId() {
        return "fetchResults";
    }
}
