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

package org.apache.flink.runtime.webmonitor.retriever.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.util.Preconditions;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * {@link LeaderGatewayRetriever} implementation using the {@link RpcService}.
 *
 * @param <T> type of the gateway to retrieve
 */
public class RpcGatewayRetriever<T extends RpcGateway> extends LeaderGatewayRetriever<T> {

	private final RpcService rpcService;
	private final Class<T> gatewayType;

	public RpcGatewayRetriever(RpcService rpcService, Class<T> gatewayType) {
		this.rpcService = Preconditions.checkNotNull(rpcService);
		this.gatewayType = Preconditions.checkNotNull(gatewayType);
	}

	@Override
	protected CompletableFuture<T> createGateway(CompletableFuture<Tuple2<String, UUID>> leaderFuture) {
		return leaderFuture.thenCompose(
			(Tuple2<String, UUID> addressLeaderTuple) -> rpcService.connect(addressLeaderTuple.f0, gatewayType));
	}
}
