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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpoint;
import org.apache.flink.table.gateway.rest.handler.operation.CancelOperationHandler;
import org.apache.flink.table.gateway.rest.handler.operation.CloseOperationHandler;
import org.apache.flink.table.gateway.rest.handler.operation.GetOperationStatusHandler;
import org.apache.flink.table.gateway.rest.handler.session.CloseSessionHandler;
import org.apache.flink.table.gateway.rest.handler.session.ConfigureSessionHandler;
import org.apache.flink.table.gateway.rest.handler.session.GetSessionConfigHandler;
import org.apache.flink.table.gateway.rest.handler.session.OpenSessionHandler;
import org.apache.flink.table.gateway.rest.handler.session.TriggerSessionHeartbeatHandler;
import org.apache.flink.table.gateway.rest.handler.statement.CompleteStatementHandler;
import org.apache.flink.table.gateway.rest.handler.statement.ExecuteStatementHandler;
import org.apache.flink.table.gateway.rest.handler.statement.FetchResultsHandler;
import org.apache.flink.table.gateway.rest.handler.util.GetApiVersionHandler;
import org.apache.flink.table.gateway.rest.handler.util.GetInfoHandler;
import org.apache.flink.table.gateway.rest.header.operation.CancelOperationHeaders;
import org.apache.flink.table.gateway.rest.header.operation.CloseOperationHeaders;
import org.apache.flink.table.gateway.rest.header.operation.GetOperationStatusHeaders;
import org.apache.flink.table.gateway.rest.header.session.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.ConfigureSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.GetSessionConfigHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.TriggerSessionHeartbeatHeaders;
import org.apache.flink.table.gateway.rest.header.statement.CompleteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.header.util.GetApiVersionHeaders;
import org.apache.flink.table.gateway.rest.header.util.GetInfoHeaders;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The rest endpoint for sql gateway. */
public class SqlGatewayRestEndpoint extends RestServerEndpoint implements SqlGatewayEndpoint {

    public final SqlGatewayService service;

    public SqlGatewayRestEndpoint(Configuration configuration, SqlGatewayService sqlGatewayService)
            throws IOException, ConfigurationException {
        super(configuration);
        service = sqlGatewayService;
    }

    @Override
    protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(
            CompletableFuture<String> localAddressFuture) {
        List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>();
        addSessionRelatedHandlers(handlers);
        addOperationRelatedHandlers(handlers);
        addUtilRelatedHandlers(handlers);
        addStatementRelatedHandlers(handlers);
        return handlers;
    }

    private void addSessionRelatedHandlers(
            List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers) {
        // Open a session
        OpenSessionHandler openSessionHandler =
                new OpenSessionHandler(service, responseHeaders, OpenSessionHeaders.getInstance());
        handlers.add(Tuple2.of(OpenSessionHeaders.getInstance(), openSessionHandler));

        // Close a session
        CloseSessionHandler closeSessionHandler =
                new CloseSessionHandler(
                        service, responseHeaders, CloseSessionHeaders.getInstance());
        handlers.add(Tuple2.of(CloseSessionHeaders.getInstance(), closeSessionHandler));

        // Configure session
        ConfigureSessionHandler configureSessionHandler =
                new ConfigureSessionHandler(
                        service, responseHeaders, ConfigureSessionHeaders.getInstance());
        handlers.add(Tuple2.of(ConfigureSessionHeaders.getInstance(), configureSessionHandler));

        // Get session configuration
        GetSessionConfigHandler getSessionConfigHandler =
                new GetSessionConfigHandler(
                        service, responseHeaders, GetSessionConfigHeaders.getInstance());
        handlers.add(Tuple2.of(GetSessionConfigHeaders.getInstance(), getSessionConfigHandler));

        // Trigger session heartbeat
        TriggerSessionHeartbeatHandler triggerSessionHeartbeatHandler =
                new TriggerSessionHeartbeatHandler(
                        service, responseHeaders, TriggerSessionHeartbeatHeaders.getInstance());
        handlers.add(
                Tuple2.of(
                        TriggerSessionHeartbeatHeaders.getInstance(),
                        triggerSessionHeartbeatHandler));
    }

    protected void addOperationRelatedHandlers(
            List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers) {

        // Get the status of operation
        GetOperationStatusHandler getOperationStatusHandler =
                new GetOperationStatusHandler(
                        service, responseHeaders, GetOperationStatusHeaders.getInstance());
        handlers.add(Tuple2.of(GetOperationStatusHeaders.getInstance(), getOperationStatusHandler));

        // Cancel the operation
        CancelOperationHandler cancelOperationHandler =
                new CancelOperationHandler(
                        service, responseHeaders, CancelOperationHeaders.getInstance());
        handlers.add(Tuple2.of(CancelOperationHeaders.getInstance(), cancelOperationHandler));

        // Close the operation
        CloseOperationHandler closeOperationHandler =
                new CloseOperationHandler(
                        service, responseHeaders, CloseOperationHeaders.getInstance());
        handlers.add(Tuple2.of(CloseOperationHeaders.getInstance(), closeOperationHandler));
    }

    protected void addUtilRelatedHandlers(
            List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers) {

        // Get info
        GetInfoHandler getInfoHandler =
                new GetInfoHandler(service, responseHeaders, GetInfoHeaders.getInstance());
        handlers.add(Tuple2.of(GetInfoHeaders.getInstance(), getInfoHandler));
        // Get version
        GetApiVersionHandler getApiVersionHandler =
                new GetApiVersionHandler(
                        service, responseHeaders, GetApiVersionHeaders.getInstance());
        handlers.add(Tuple2.of(GetApiVersionHeaders.getInstance(), getApiVersionHandler));
    }

    private void addStatementRelatedHandlers(
            List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers) {
        // Complete a statement
        CompleteStatementHandler completeStatementHandler =
                new CompleteStatementHandler(
                        service, responseHeaders, CompleteStatementHeaders.getInstance());
        handlers.add(Tuple2.of(CompleteStatementHeaders.getInstance(), completeStatementHandler));

        // Execute a statement
        ExecuteStatementHandler executeStatementHandler =
                new ExecuteStatementHandler(
                        service, responseHeaders, ExecuteStatementHeaders.getInstance());
        handlers.add(Tuple2.of(ExecuteStatementHeaders.getInstance(), executeStatementHandler));

        // Fetch results
        handlers.add(
                Tuple2.of(
                        FetchResultsHeaders.getDefaultInstance(),
                        new FetchResultsHandler(
                                service,
                                responseHeaders,
                                FetchResultsHeaders.getDefaultInstance())));
        handlers.add(
                Tuple2.of(
                        FetchResultsHeaders.getInstanceV1(),
                        new FetchResultsHandler(
                                service, responseHeaders, FetchResultsHeaders.getInstanceV1())));
    }

    @Override
    protected void startInternal() {}

    @Override
    public void stop() throws Exception {
        super.close();
    }
}
