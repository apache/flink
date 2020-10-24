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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A base {@link DispatcherLeaderProcess}.
 */
@Internal
public abstract class AbstractDispatcherLeaderProcess implements DispatcherLeaderProcess {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final Object lock = new Object();

	private final UUID leaderSessionId;

	private final FatalErrorHandler fatalErrorHandler;

	private final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture;

	private final CompletableFuture<String> leaderAddressFuture;

	private final CompletableFuture<Void> terminationFuture;

	private final CompletableFuture<ApplicationStatus> shutDownFuture;

	private State state;

	@Nullable
	private DispatcherGatewayService dispatcherService;

	AbstractDispatcherLeaderProcess(UUID leaderSessionId, FatalErrorHandler fatalErrorHandler) {
		this.leaderSessionId = leaderSessionId;
		this.fatalErrorHandler = fatalErrorHandler;

		this.dispatcherGatewayFuture = new CompletableFuture<>();
		this.leaderAddressFuture = dispatcherGatewayFuture.thenApply(RestfulGateway::getAddress);
		this.terminationFuture = new CompletableFuture<>();
		this.shutDownFuture = new CompletableFuture<>();

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
	public final CompletableFuture<String> getLeaderAddressFuture() {
		return leaderAddressFuture;
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	protected final Optional<DispatcherGatewayService> getDispatcherService() {
		return Optional.ofNullable(dispatcherService);
	}

	@Override
	public final CompletableFuture<Void> closeAsync() {
		runIfStateIsNot(
			State.STOPPED,
			this::closeInternal);

		return terminationFuture;
	}

	private void closeInternal() {
		log.info("Stopping {}.", getClass().getSimpleName());

		state = State.STOPPED;

		final CompletableFuture<Void> dispatcherServiceTerminationFuture = closeDispatcherService();

		final CompletableFuture<Void> onCloseTerminationFuture = FutureUtils.composeAfterwards(
			dispatcherServiceTerminationFuture,
			this::onClose);

		FutureUtils.forward(
			onCloseTerminationFuture,
			this.terminationFuture);
	}

	private CompletableFuture<Void> closeDispatcherService() {
		if (dispatcherService != null) {
			return dispatcherService.closeAsync();
		} else {
			return FutureUtils.completedVoidFuture();
		}
	}

	protected abstract void onStart();

	protected CompletableFuture<Void> onClose() {
		return FutureUtils.completedVoidFuture();
	}

	final void completeDispatcherSetup(DispatcherGatewayService dispatcherService) {
		runIfStateIs(
			State.RUNNING,
			() -> completeDispatcherSetupInternal(dispatcherService));
	}

	private void completeDispatcherSetupInternal(DispatcherGatewayService createdDispatcherService) {
		Preconditions.checkState(dispatcherService == null, "The DispatcherGatewayService can only be set once.");
		dispatcherService = createdDispatcherService;
		dispatcherGatewayFuture.complete(createdDispatcherService.getGateway());
		FutureUtils.forward(createdDispatcherService.getShutDownFuture(), shutDownFuture);
		handleUnexpectedDispatcherServiceTermination(createdDispatcherService);
	}

	private void handleUnexpectedDispatcherServiceTermination(DispatcherGatewayService createdDispatcherService) {
		createdDispatcherService.getTerminationFuture().whenComplete(
				(ignored, throwable) -> {
					runIfStateIs(State.RUNNING, () -> {
						handleError(new FlinkException("Unexpected termination of DispatcherService.", throwable));
					});
				});
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
			handleError(throwable);
		}

		return null;
	}

	private void handleError(Throwable throwable) {
		closeAsync();
		fatalErrorHandler.onFatalError(throwable);
	}

	/**
	 * The state of the {@link DispatcherLeaderProcess}.
	 */
	protected enum State {
		CREATED,
		RUNNING,
		STOPPED
	}

	// ------------------------------------------------------------
	// Internal classes
	// ------------------------------------------------------------

	/**
	 * Factory for {@link DispatcherGatewayService}.
	 */
	public interface DispatcherGatewayServiceFactory {
		DispatcherGatewayService create(
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			JobGraphWriter jobGraphWriter);
	}

	/**
	 * An accessor of the {@link DispatcherGateway}.
	 */
	public interface DispatcherGatewayService extends AutoCloseableAsync {
		DispatcherGateway getGateway();

		CompletableFuture<Void> onRemovedJobGraph(JobID jobId);

		CompletableFuture<ApplicationStatus> getShutDownFuture();

		CompletableFuture<Void> getTerminationFuture();
	}
}
