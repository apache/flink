/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.dataset;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.resourcemanager.AbstractResourceManagerHandler;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetListHeaders;
import org.apache.flink.runtime.rest.messages.dataset.ClusterDataSetListResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler for {@link ClusterDataSetListHeaders}. */
public class ClusterDataSetListHandler
        extends AbstractResourceManagerHandler<
                RestfulGateway,
                EmptyRequestBody,
                ClusterDataSetListResponseBody,
                EmptyMessageParameters> {
    public ClusterDataSetListHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                ClusterDataSetListHeaders.INSTANCE,
                resourceManagerGatewayRetriever);
    }

    @Override
    protected CompletableFuture<ClusterDataSetListResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request,
            @Nonnull ResourceManagerGateway gateway)
            throws RestHandlerException {
        return gateway.listDataSets().thenApply(ClusterDataSetListResponseBody::from);
    }
}
