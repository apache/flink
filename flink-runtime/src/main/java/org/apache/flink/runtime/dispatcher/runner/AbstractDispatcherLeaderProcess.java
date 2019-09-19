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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

abstract class AbstractDispatcherLeaderProcess implements DispatcherLeaderProcess {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final Object lock = new Object();

	private final UUID leaderSessionId;

	private final FatalErrorHandler fatalErrorHandler;

	private final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture;

	private final CompletableFuture<String> confirmLeaderSessionFuture;

	private final CompletableFuture<Void> terminationFuture;

	private State state;

	AbstractDispatcherLeaderProcess(UUID leaderSessionId, FatalErrorHandler fatalErrorHandler) {
		this.leaderSessionId = leaderSessionId;
		this.fatalErrorHandler = fatalErrorHandler;

		this.dispatcherGatewayFuture = new CompletableFuture<>();
		this.confirmLeaderSessionFuture = dispatcherGatewayFuture.thenApply(RestfulGateway::getAddress);
		this.terminationFuture = new CompletableFuture<>();

		this.state = State.CREATED;
	}

	@VisibleForTesting
	State getState() {
		synchronized (lock) {
			return state;
		}
	}

	@Override
	public final void start() {
		runIfStateIs(
			State.CREATED,
			this::startInternal);
	}

	private void startInternal() {
		log.info("Start {}.", getClass().getSimpleName());
		state = State.RUNNING;
		onStart();
	}

	@Override
	public final UUID getLeaderSessionId() {
		return leaderSessionId;
	}

	@Override
	public final CompletableFuture<DispatcherGateway> getDispatcherGateway() {
		return dispatcherGatewayFuture;
	}

	@Override
	public final CompletableFuture<String> getConfirmLeaderSessionFuture() {
		return confirmLeaderSessionFuture;
	}

	@Override
	public final CompletableFuture<Void> closeAsync() {
		runIfStateIsNot(
			State.STOPPED,
			this::closeInternal);

		return terminationFuture;
	}

	private void closeInternal() {
		FutureUtils.forward(
			onClose(),
			terminationFuture);

		state = State.STOPPED;
	}

	protected abstract void onStart();

	protected CompletableFuture<Void> onClose() {
		return FutureUtils.completedVoidFuture();
	}

	protected void completeDispatcherGatewayFuture(DispatcherGateway dispatcherGateway) {
		dispatcherGatewayFuture.complete(dispatcherGateway);
	}

	final <V> Optional<V> supplyUnsynchronizedIfRunning(Supplier<V> supplier) {
		synchronized (lock) {
			if (state != State.RUNNING) {
				return Optional.empty();
			}
		}

		return Optional.of(supplier.get());
	}

	final <V> Optional<V> supplyIfRunning(Supplier<V> supplier) {
		synchronized (lock) {
			if (state != State.RUNNING) {
				return Optional.empty();
			}

			return Optional.of(supplier.get());
		}
	}

	final void runIfStateIs(State expectedState, Runnable action) {
		runIfState(expectedState::equals, action);
	}

	private void runIfStateIsNot(State notExpectedState, Runnable action) {
		runIfState(
			state -> !notExpectedState.equals(state),
			action);
	}

	private void runIfState(Predicate<State> actionPredicate, Runnable action) {
		synchronized (lock) {
			if (actionPredicate.test(state)) {
				action.run();
			}
		}
	}

	final <T> Void onErrorIfRunning(T ignored, Throwable throwable) {
		synchronized (lock) {
			if (state != State.RUNNING) {
				return null;
			}
		}

		if (throwable != null) {
			closeAsync();
			fatalErrorHandler.onFatalError(throwable);
		}

		return null;
	}

	protected enum State {
		CREATED,
		RUNNING,
		STOPPED
	}
}
