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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * {@link LeaderGatewayRetriever} implementation using the {@link RpcService}.
 *
 * @param <F> type of the fencing token
 * @param <T> type of the fenced gateway to retrieve
 */
public class RpcGatewayRetriever<F extends Serializable, T extends FencedRpcGateway<F>> extends LeaderGatewayRetriever<T> {

	private final RpcService rpcService;
	private final Class<T> gatewayType;
	private final Function<UUID, F> fencingTokenMapper;

	private final int retries;
	private final Time retryDelay;

	public RpcGatewayRetriever(
			RpcService rpcService,
			Class<T> gatewayType,
			Function<UUID, F> fencingTokenMapper,
			int retries,
			Time retryDelay) {
		this.rpcService = Preconditions.checkNotNull(rpcService);

		this.gatewayType = Preconditions.checkNotNull(gatewayType);
		this.fencingTokenMapper = Preconditions.checkNotNull(fencingTokenMapper);

		Preconditions.checkArgument(retries >= 0, "The number of retries must be greater or equal to 0.");
		this.retries = retries;
		this.retryDelay = Preconditions.checkNotNull(retryDelay);
	}

	@Override
	protected CompletableFuture<T> createGateway(CompletableFuture<Tuple2<String, UUID>> leaderFuture) {
		return FutureUtils.retryWithDelay(
			() ->
				leaderFuture.thenCompose(
					(Tuple2<String, UUID> addressLeaderTuple) ->
						rpcService.connect(
							addressLeaderTuple.f0,
							fencingTokenMapper.apply(addressLeaderTuple.f1),
							gatewayType)),
			retries,
			retryDelay,
			rpcService.getScheduledExecutor());
	}
}
