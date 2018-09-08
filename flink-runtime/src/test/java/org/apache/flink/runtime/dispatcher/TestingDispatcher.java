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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * {@link Dispatcher} implementation used for testing purposes.
 */
class TestingDispatcher extends Dispatcher {

	TestingDispatcher(
		RpcService rpcService,
		String endpointId,
		Configuration configuration,
		HighAvailabilityServices highAvailabilityServices,
		ResourceManagerGateway resourceManagerGateway,
		BlobServer blobServer,
		HeartbeatServices heartbeatServices,
		JobManagerMetricGroup jobManagerMetricGroup,
		@Nullable String metricQueryServicePath,
		ArchivedExecutionGraphStore archivedExecutionGraphStore,
		JobManagerRunnerFactory jobManagerRunnerFactory,
		FatalErrorHandler fatalErrorHandler) throws Exception {
		super(
			rpcService,
			endpointId,
			configuration,
			highAvailabilityServices,
			highAvailabilityServices.getSubmittedJobGraphStore(),
			resourceManagerGateway,
			blobServer,
			heartbeatServices,
			jobManagerMetricGroup,
			metricQueryServicePath,
			archivedExecutionGraphStore,
			jobManagerRunnerFactory,
			fatalErrorHandler,
			null,
			VoidHistoryServerArchivist.INSTANCE);
	}

	void completeJobExecution(ArchivedExecutionGraph archivedExecutionGraph) {
		runAsync(
			() -> jobReachedGloballyTerminalState(archivedExecutionGraph));
	}

	CompletableFuture<Void> getJobTerminationFuture(@Nonnull JobID jobId, @Nonnull Time timeout) {
		return callAsyncWithoutFencing(
			() -> getJobTerminationFuture(jobId),
			timeout).thenCompose(Function.identity());
	}

	CompletableFuture<Void> getRecoverOperationFuture(@Nonnull Time timeout) {
		return callAsyncWithoutFencing(
			this::getRecoveryOperation,
			timeout).thenCompose(Function.identity());
	}
}
