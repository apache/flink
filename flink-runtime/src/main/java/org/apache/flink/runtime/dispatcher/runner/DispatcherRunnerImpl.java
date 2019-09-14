/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobGraphStore;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Runner responsible for executing a {@link Dispatcher} or a subclass thereof.
 */
class DispatcherRunnerImpl implements DispatcherRunner {

	private final Dispatcher dispatcher;

	private final LeaderRetrievalService leaderRetrievalService;

	private final LeaderRetriever leaderRetriever;

	DispatcherRunnerImpl(
		DispatcherFactory dispatcherFactory,
		RpcService rpcService,
		PartialDispatcherServicesWithJobGraphStore partialDispatcherServicesWithJobGraphStore) throws Exception {
		this.dispatcher = dispatcherFactory.createDispatcher(
			rpcService,
			Collections.emptyList(),
			partialDispatcherServicesWithJobGraphStore);
		this.leaderRetrievalService = partialDispatcherServicesWithJobGraphStore.getHighAvailabilityServices().getDispatcherLeaderRetriever();
		this.leaderRetriever = new LeaderRetriever();

		leaderRetrievalService.start(leaderRetriever);
		dispatcher.start();
	}

	@Override
	public CompletableFuture<DispatcherGateway> getDispatcherGateway() {
		return leaderRetriever.getLeaderFuture().thenApply(ignored -> dispatcher.getSelfGateway(DispatcherGateway.class));
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		Exception exception = null;

		try {
			leaderRetrievalService.stop();
		} catch (Exception e) {
			exception = e;
		}

		Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>();

		terminationFutures.add(dispatcher.closeAsync());

		if (exception != null) {
			terminationFutures.add(FutureUtils.completedExceptionally(exception));
		}

		return FutureUtils.completeAll(terminationFutures);
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return dispatcher.getShutDownFuture();
	}
}
