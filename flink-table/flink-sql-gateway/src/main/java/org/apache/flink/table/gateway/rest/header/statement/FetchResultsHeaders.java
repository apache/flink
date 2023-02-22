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
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.operation.OperationHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsTokenPathParameter;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion.V1;

/** Message headers for fetching results. */
public class FetchResultsHeaders
        implements SqlGatewayMessageHeaders<
                EmptyRequestBody, FetchResultsResponseBody, FetchResultsMessageParameters> {

    private static final FetchResultsHeaders INSTANCE_V1 = new FetchResultsHeaders(V1);
    private static final FetchResultsHeaders DEFAULT_INSTANCE =
            new FetchResultsHeaders(SqlGatewayRestAPIVersion.getDefaultVersion());

    public static final String URL =
            "/sessions/:"
                    + SessionHandleIdPathParameter.KEY
                    + "/operations/:"
                    + OperationHandleIdPathParameter.KEY
                    + "/result/:"
                    + FetchResultsTokenPathParameter.KEY;

    private final SqlGatewayRestAPIVersion version;

    private FetchResultsHeaders(SqlGatewayRestAPIVersion version) {
        this.version = version;
    }

    public static FetchResultsHeaders getInstanceV1() {
        return INSTANCE_V1;
    }

    public static FetchResultsHeaders getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    public static @Nullable String buildNextUri(
            SqlGatewayRestAPIVersion version,
            String sessionId,
            String operationId,
            Long nextToken,
            RowFormat rowFormat) {
        if (nextToken == null) {
            return null;
        }

        if (version == V1) {
            return String.format(
                    "/%s/sessions/%s/operations/%s/result/%s",
                    version.getURLVersionPrefix(), sessionId, operationId, nextToken);
        } else {
            return String.format(
                    "/%s/sessions/%s/operations/%s/result/%s?rowFormat=%s",
                    version.getURLVersionPrefix(), sessionId, operationId, nextToken, rowFormat);
        }
    }

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

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        if (version == V1) {
            return Collections.singleton(V1);
        } else {
            return Arrays.stream(SqlGatewayRestAPIVersion.values())
                    .filter(SqlGatewayRestAPIVersion::isStableVersion)
                    .filter(version -> version != V1)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public FetchResultsMessageParameters getUnresolvedMessageParameters() {
        return new FetchResultsMessageParameters(version);
    }

    @Override
    public String operationId() {
        return "fetchResults";
    }
}
