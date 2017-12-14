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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;

import java.util.concurrent.CompletableFuture;

/**
 * {@link MetricQueryServiceRetriever} implementation for Akka based {@link MetricQueryService}.
 */
public class AkkaQueryServiceRetriever implements MetricQueryServiceRetriever {
	private final ActorSystem actorSystem;
	private final Time lookupTimeout;

	public AkkaQueryServiceRetriever(ActorSystem actorSystem, Time lookupTimeout) {
		this.actorSystem = Preconditions.checkNotNull(actorSystem);
		this.lookupTimeout = Preconditions.checkNotNull(lookupTimeout);
	}

	@Override
	public CompletableFuture<MetricQueryServiceGateway> retrieveService(String queryServicePath) {
		ActorSelection selection = actorSystem.actorSelection(queryServicePath);

		return FutureUtils.toJava(selection.resolveOne(FutureUtils.toFiniteDuration(lookupTimeout))).thenApply(AkkaQueryServiceGateway::new);
	}
}
