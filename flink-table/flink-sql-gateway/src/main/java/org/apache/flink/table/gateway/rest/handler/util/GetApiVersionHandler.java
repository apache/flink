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

package org.apache.flink.table.gateway.rest.handler.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.util.GetApiVersionResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Handler to get rest api version. */
public class GetApiVersionHandler
        extends AbstractSqlGatewayRestHandler<
                EmptyRequestBody, GetApiVersionResponseBody, EmptyMessageParameters> {

    private final List<SqlGatewayRestAPIVersion> stableVersions;

    public GetApiVersionHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, GetApiVersionResponseBody, EmptyMessageParameters>
                    messageHeaders) {
        this(
                service,
                responseHeaders,
                messageHeaders,
                SqlGatewayRestAPIVersion.getStableVersions());
    }

    @VisibleForTesting
    public GetApiVersionHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, GetApiVersionResponseBody, EmptyMessageParameters>
                    messageHeaders,
            List<SqlGatewayRestAPIVersion> stableVersions) {
        super(service, responseHeaders, messageHeaders);
        this.stableVersions = stableVersions;
    }

    @Override
    protected CompletableFuture<GetApiVersionResponseBody> handleRequest(
            @Nullable SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<EmptyRequestBody> request) {
        return CompletableFuture.completedFuture(
                new GetApiVersionResponseBody(
                        stableVersions.stream().map(Enum::name).collect(Collectors.toList())));
    }
}
