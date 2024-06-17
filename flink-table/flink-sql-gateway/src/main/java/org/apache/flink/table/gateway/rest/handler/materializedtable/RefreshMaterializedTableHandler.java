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

package org.apache.flink.table.gateway.rest.handler.materializedtable;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.materializedtable.MaterializedTableIdentifierPathParameter;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableParameters;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionHandleIdPathParameter;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to execute materialized table refresh operation. */
public class RefreshMaterializedTableHandler
        extends AbstractSqlGatewayRestHandler<
                RefreshMaterializedTableRequestBody,
                RefreshMaterializedTableResponseBody,
                RefreshMaterializedTableParameters> {

    public RefreshMaterializedTableHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            RefreshMaterializedTableRequestBody,
                            RefreshMaterializedTableResponseBody,
                            RefreshMaterializedTableParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<RefreshMaterializedTableResponseBody> handleRequest(
            @Nullable SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<RefreshMaterializedTableRequestBody> request)
            throws RestHandlerException {
        try {
            SessionHandle sessionHandle =
                    request.getPathParameter(SessionHandleIdPathParameter.class);
            String materializedTableIdentifier =
                    request.getPathParameter(MaterializedTableIdentifierPathParameter.class);
            boolean isPeriodic = request.getRequestBody().isPeriodic();
            String scheduleTime = request.getRequestBody().getScheduleTime();
            Map<String, String> dynamicOptions = request.getRequestBody().getDynamicOptions();
            Map<String, String> staticPartitions = request.getRequestBody().getStaticPartitions();
            Map<String, String> executionConfig = request.getRequestBody().getExecutionConfig();
            OperationHandle operationHandle =
                    service.refreshMaterializedTable(
                            sessionHandle,
                            materializedTableIdentifier,
                            isPeriodic,
                            scheduleTime,
                            dynamicOptions == null ? Collections.emptyMap() : dynamicOptions,
                            staticPartitions == null ? Collections.emptyMap() : staticPartitions,
                            executionConfig == null ? Collections.emptyMap() : executionConfig);

            return CompletableFuture.completedFuture(
                    new RefreshMaterializedTableResponseBody(
                            operationHandle.getIdentifier().toString()));
        } catch (Exception e) {
            throw new RestHandlerException(
                    e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}
