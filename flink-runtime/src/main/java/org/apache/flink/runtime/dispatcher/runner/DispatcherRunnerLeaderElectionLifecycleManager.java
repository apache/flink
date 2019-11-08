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
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

final class DispatcherRunnerLeaderElectionLifecycleManager<T extends DispatcherRunner & LeaderContender> implements DispatcherRunner {
	private final T dispatcherRunner;
	private final LeaderElectionService leaderElectionService;

	private DispatcherRunnerLeaderElectionLifecycleManager(T dispatcherRunner, LeaderElectionService leaderElectionService) throws Exception {
		this.dispatcherRunner = dispatcherRunner;
		this.leaderElectionService = leaderElectionService;

		leaderElectionService.start(dispatcherRunner);
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return dispatcherRunner.getShutDownFuture();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		final CompletableFuture<Void> servicesTerminationFuture = stopServices();
		final CompletableFuture<Void> dispatcherRunnerTerminationFuture = dispatcherRunner.closeAsync();

		return FutureUtils.completeAll(Arrays.asList(servicesTerminationFuture, dispatcherRunnerTerminationFuture));
	}

	private CompletableFuture<Void> stopServices() {
		try {
			leaderElectionService.stop();
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}

		return FutureUtils.completedVoidFuture();
	}

	public static <T extends DispatcherRunner & LeaderContender> DispatcherRunner createFor(T dispatcherRunner, LeaderElectionService leaderElectionService) throws Exception {
		return new DispatcherRunnerLeaderElectionLifecycleManager<>(dispatcherRunner, leaderElectionService);
	}
}
