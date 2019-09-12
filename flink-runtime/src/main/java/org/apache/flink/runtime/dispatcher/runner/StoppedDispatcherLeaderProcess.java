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

import org.apache.flink.runtime.dispatcher.DispatcherGateway;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * {@link DispatcherLeaderProcess} implementation which is stopped. This class
 * is useful as the initial state of the {@link DispatcherRunnerImplNG}.
 */
public enum StoppedDispatcherLeaderProcess implements DispatcherLeaderProcess {
	INSTANCE;

	private static final CompletableFuture<Void> TERMINATION_FUTURE = CompletableFuture.completedFuture(null);
	private static final UUID LEADER_SESSION_ID = new UUID(0L, 0L);
	private static final CompletableFuture<String> NEVER_COMPLETED_FUTURE = new CompletableFuture<>();

	@Override
	public void start() {

	}

	@Override
	public UUID getLeaderSessionId() {
		return LEADER_SESSION_ID;
	}

	@Override
	public CompletableFuture<DispatcherGateway> getDispatcherGateway() {
		return null;
	}

	@Override
	public CompletableFuture<String> getConfirmLeaderSessionFuture() {
		return NEVER_COMPLETED_FUTURE;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return TERMINATION_FUTURE;
	}
}
