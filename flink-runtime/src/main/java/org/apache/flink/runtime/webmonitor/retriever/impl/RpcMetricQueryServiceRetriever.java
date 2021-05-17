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

package org.apache.flink.runtime.webmonitor.retriever.impl;

import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;

import java.util.concurrent.CompletableFuture;

/** {@link MetricQueryServiceRetriever} implementation for rpc based {@link MetricQueryService}. */
public class RpcMetricQueryServiceRetriever implements MetricQueryServiceRetriever {

    private RpcService rpcService;

    public RpcMetricQueryServiceRetriever(RpcService rpcService) {
        this.rpcService = rpcService;
    }

    @Override
    public CompletableFuture<MetricQueryServiceGateway> retrieveService(String rpcServiceAddress) {
        return rpcService.connect(rpcServiceAddress, MetricQueryServiceGateway.class);
    }
}
