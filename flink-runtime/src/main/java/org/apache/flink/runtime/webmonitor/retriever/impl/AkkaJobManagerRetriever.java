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
import org.apache.flink.runtime.akka.AkkaJobManagerGateway;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.webmonitor.retriever.JobManagerRetriever;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * {@link JobManagerRetriever} implementation for Akka based JobManagers.
 */
public class AkkaJobManagerRetriever extends JobManagerRetriever {

	private final ActorSystem actorSystem;
	private final Time timeout;

	public AkkaJobManagerRetriever(
			ActorSystem actorSystem,
			Time timeout) {

		this.actorSystem = Preconditions.checkNotNull(actorSystem);
		this.timeout = Preconditions.checkNotNull(timeout);
	}

	@Override
	protected CompletableFuture<JobManagerGateway> createJobManagerGateway(String leaderAddress, UUID leaderId) throws Exception {
		return FutureUtils.toJava(
			AkkaUtils.getActorRefFuture(
				leaderAddress,
				actorSystem,
				FutureUtils.toFiniteDuration(timeout)))
			.thenApplyAsync(
				(ActorRef jobManagerRef) -> {
					ActorGateway leaderGateway = new AkkaActorGateway(
						jobManagerRef, leaderId);

					return new AkkaJobManagerGateway(leaderGateway);
				},
				actorSystem.dispatcher());
	}
}
