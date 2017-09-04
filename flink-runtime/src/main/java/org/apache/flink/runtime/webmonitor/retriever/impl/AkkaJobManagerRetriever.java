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
import org.apache.flink.runtime.akka.AkkaJobManagerGateway;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.akka.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * {@link LeaderGatewayRetriever} implementation for Akka based JobManagers.
 */
public class AkkaJobManagerRetriever extends LeaderGatewayRetriever<JobManagerGateway> {

	private final ActorSystem actorSystem;
	private final Time timeout;
	private final int retries;
	private final Time retryDelay;

	private final ScheduledExecutor scheduledExecutor;

	public AkkaJobManagerRetriever(
			ActorSystem actorSystem,
			Time timeout,
			int retries,
			Time retryDelay) {

		this.actorSystem = Preconditions.checkNotNull(actorSystem);
		this.timeout = Preconditions.checkNotNull(timeout);

		Preconditions.checkArgument(retries >= 0, "The number of retries must be >= 0.");
		this.retries = retries;

		this.retryDelay = Preconditions.checkNotNull(retryDelay);

		this.scheduledExecutor = new ActorSystemScheduledExecutorAdapter(actorSystem);
	}

	@Override
	protected CompletableFuture<JobManagerGateway> createGateway(CompletableFuture<Tuple2<String, UUID>> leaderFuture) {

		return FutureUtils.retryWithDelay(
			() ->
				leaderFuture.thenCompose(
					(Tuple2<String, UUID> addressLeaderId) ->
						FutureUtils.toJava(
							AkkaUtils.getActorRefFuture(
								addressLeaderId.f0,
								actorSystem,
								FutureUtils.toFiniteDuration(timeout)))
							.thenApply(
								(ActorRef jobManagerRef) -> {
									ActorGateway leaderGateway = new AkkaActorGateway(
										jobManagerRef,
										addressLeaderId.f1);

									return new AkkaJobManagerGateway(leaderGateway);
								}
							)),
			retries,
			retryDelay,
			scheduledExecutor);
	}
}
