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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Process which encapsulates the job recovery logic and life cycle management of a
 * {@link Dispatcher}.
 */
public class DispatcherLeaderProcessImpl implements DispatcherLeaderProcess, JobGraphStore.JobGraphListener {

	private static final Logger LOG = LoggerFactory.getLogger(DispatcherLeaderProcessImpl.class);

	private final Object lock = new Object();

	private final UUID leaderSessionId;

	private final DispatcherServiceFactory dispatcherFactory;

	private final JobGraphStore jobGraphStore;

	private final Executor ioExecutor;

	private final FatalErrorHandler fatalErrorHandler;

	private final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture;

	private final CompletableFuture<String> confirmLeaderSessionFuture;

	private final CompletableFuture<Void> terminationFuture;

	private State state;

	@Nullable
	private DispatcherService dispatcher;

	private CompletableFuture<Void> onGoingRecoveryOperation = FutureUtils.completedVoidFuture();

	private DispatcherLeaderProcessImpl(
			UUID leaderSessionId,
			DispatcherServiceFactory dispatcherFactory,
			JobGraphStore jobGraphStore,
			Executor ioExecutor,
			FatalErrorHandler fatalErrorHandler) {
		this.leaderSessionId = leaderSessionId;
		this.dispatcherFactory = dispatcherFactory;
		this.jobGraphStore = jobGraphStore;
		this.ioExecutor = ioExecutor;
		this.fatalErrorHandler = fatalErrorHandler;

		this.dispatcherGatewayFuture = new CompletableFuture<>();
		this.confirmLeaderSessionFuture = dispatcherGatewayFuture.thenApply(RestfulGateway::getAddress);
		this.terminationFuture = new CompletableFuture<>();

		this.state = State.CREATED;
		this.dispatcher = null;
	}

	State getState() {
		synchronized (lock) {
			return state;
		}
	}

	@Override
	public void start() {
		runIfStateIs(
			State.CREATED,
			this::startInternal);
	}

	private void startInternal() {
		LOG.info("Start {}.", getClass().getSimpleName());
		state = State.RUNNING;
		startServices();

		onGoingRecoveryOperation = recoverJobsAsync()
			.thenAccept(this::createDispatcherIfRunning)
			.handle(this::onErrorIfRunning);
	}

	private void startServices() {
		try {
			jobGraphStore.start(this);
		} catch (Exception e) {
			throw new FlinkRuntimeException(
				String.format(
					"Could not start %s when trying to start the %s.",
					jobGraphStore.getClass().getSimpleName(),
					getClass().getSimpleName()),
				e);
		}
	}

	private <T> Void onErrorIfRunning(T ignored, Throwable throwable) {
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

	private void createDispatcherIfRunning(Collection<JobGraph> jobGraphs) {
		runIfStateIs(State.RUNNING, () -> createDispatcher(jobGraphs));
	}

	private void createDispatcher(Collection<JobGraph> jobGraphs) {
		dispatcher = dispatcherFactory.create(jobGraphs, jobGraphStore);
		dispatcherGatewayFuture.complete(dispatcher.getGateway());
	}

	private CompletableFuture<Collection<JobGraph>> recoverJobsAsync() {
		return CompletableFuture.supplyAsync(
			this::recoverJobsIfRunning,
			ioExecutor);
	}

	private Collection<JobGraph> recoverJobsIfRunning() {
		return supplyUnsynchronizedIfRunning(this::recoverJobs).orElse(Collections.emptyList());

	}

	private Collection<JobGraph> recoverJobs() {
		LOG.info("Recover all persisted job graphs.");
		final Collection<JobID> jobIds = getJobIds();
		final Collection<JobGraph> recoveredJobGraphs = new ArrayList<>();

		for (JobID jobId : jobIds) {
			recoveredJobGraphs.add(recoverJob(jobId));
		}

		LOG.info("Successfully recovered {} persisted job graphs.", recoveredJobGraphs.size());

		return recoveredJobGraphs;
	}

	private Collection<JobID> getJobIds() {
		try {
			return jobGraphStore.getJobIds();
		} catch (Exception e) {
			throw new FlinkRuntimeException(
				"Could not retrieve job ids of persisted jobs.",
				e);
		}
	}

	private JobGraph recoverJob(JobID jobId) {
		LOG.info("Trying to recover job with job id {}.", jobId);
		try {
			return jobGraphStore.recoverJobGraph(jobId);
		} catch (Exception e) {
			throw new FlinkRuntimeException(
				String.format("Could not recover job with job id %s.", jobId),
				e);
		}
	}

	@Override
	public UUID getLeaderSessionId() {
		return leaderSessionId;
	}

	@Override
	public CompletableFuture<DispatcherGateway> getDispatcherGateway() {
		return dispatcherGatewayFuture;
	}

	@Override
	public CompletableFuture<String> getConfirmLeaderSessionFuture() {
		return confirmLeaderSessionFuture;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		runIfStateIsNot(
			State.STOPPED,
			this::closeInternal);

		return terminationFuture;
	}

	private void closeInternal() {
		LOG.info("Stopping {}.", getClass().getSimpleName());
		final CompletableFuture<Void> dispatcherTerminationFuture;

		if (dispatcher != null) {
			dispatcherTerminationFuture = dispatcher.closeAsync();
		} else {
			dispatcherTerminationFuture = FutureUtils.completedVoidFuture();
		}

		final CompletableFuture<Void> stopServicesFuture = FutureUtils.runAfterwardsAsync(
			dispatcherTerminationFuture,
			this::stopServices,
			ioExecutor);

		FutureUtils.forward(
			stopServicesFuture,
			terminationFuture);

		state = State.STOPPED;
	}

	private void stopServices() throws Exception {
		Exception exception = null;

		try {
			jobGraphStore.stop();
		} catch (Exception e) {
			exception = e;
		}

		ExceptionUtils.tryRethrowException(exception);
	}

	// ------------------------------------------------------------
	// JobGraphListener
	// ------------------------------------------------------------

	@Override
	public void onAddedJobGraph(JobID jobId) {
		runIfStateIs(
			State.RUNNING,
			() -> handleAddedJobGraph(jobId));
	}

	private void handleAddedJobGraph(JobID jobId) {
		LOG.debug(
			"Job {} has been added to the {} by another process.",
			jobId,
			jobGraphStore.getClass().getSimpleName());

		// serialize all ongoing recovery operations
		onGoingRecoveryOperation = onGoingRecoveryOperation
			.thenApplyAsync(
				ignored -> recoverJobIfRunning(jobId),
				ioExecutor)
			.thenCompose(optionalJobGraph -> optionalJobGraph
				.flatMap(this::submitAddedJobIfRunning)
				.orElse(FutureUtils.completedVoidFuture()))
			.handle(this::onErrorIfRunning);
	}

	private Optional<CompletableFuture<Void>> submitAddedJobIfRunning(JobGraph jobGraph) {
		return supplyIfRunning(() -> submitAddedJob(jobGraph));
	}

	private CompletableFuture<Void> submitAddedJob(JobGraph jobGraph) {
		final DispatcherGateway dispatcherGateway = getDispatcherGatewayInternal();

		// TODO: Filter out duplicate job submissions which can happen with the JobGraphListener
		return dispatcherGateway
			.submitJob(jobGraph, RpcUtils.INF_TIMEOUT)
			.thenApply(FunctionUtils.nullFn());
	}

	private DispatcherGateway getDispatcherGatewayInternal() {
		return Preconditions.checkNotNull(dispatcherGatewayFuture.getNow(null));
	}

	private Optional<JobGraph> recoverJobIfRunning(JobID jobId) {
		return supplyUnsynchronizedIfRunning(() -> recoverJob(jobId));
	}

	@Override
	public void onRemovedJobGraph(JobID jobId) {
		runIfStateIs(
			State.RUNNING,
			() -> handleRemovedJobGraph(jobId));
	}

	private void handleRemovedJobGraph(JobID jobId) {
		LOG.debug(
			"Job {} has been removed from the {} by another process.",
			jobId,
			jobGraphStore.getClass().getSimpleName());

		onGoingRecoveryOperation = onGoingRecoveryOperation
			.thenCompose(ignored -> removeJobGraphIfRunning(jobId).orElse(FutureUtils.completedVoidFuture()))
			.handle(this::onErrorIfRunning);
	}

	private Optional<CompletableFuture<Void>> removeJobGraphIfRunning(JobID jobId) {
		return supplyIfRunning(() -> removeJobGraph(jobId));
	}

	private CompletableFuture<Void> removeJobGraph(JobID jobId) {
		final DispatcherGateway dispatcherGateway = getDispatcherGatewayInternal();

		// TODO: replace cancel with other fail method
		return dispatcherGateway
			.cancelJob(jobId, RpcUtils.INF_TIMEOUT)
			.thenApply(FunctionUtils.nullFn());
	}

	// ---------------------------------------------------------------
	// Factory methods
	// ---------------------------------------------------------------

	public static DispatcherLeaderProcessImpl create(
			UUID leaderSessionId,
			DispatcherServiceFactory dispatcherFactory,
			JobGraphStore jobGraphStore,
			Executor ioExecutor,
			FatalErrorHandler fatalErrorHandler) {
		return new DispatcherLeaderProcessImpl(
			leaderSessionId,
			dispatcherFactory,
			jobGraphStore,
			ioExecutor,
			fatalErrorHandler);
	}

	// ---------------------------------------------------------------
	// Internal helper methods
	// ---------------------------------------------------------------

	private <V> Optional<V> supplyUnsynchronizedIfRunning(Supplier<V> supplier) {
		synchronized (lock) {
			if (state != State.RUNNING) {
				return Optional.empty();
			}
		}

		return Optional.of(supplier.get());
	}

	private <V> Optional<V> supplyIfRunning(Supplier<V> supplier) {
		synchronized (lock) {
			if (state != State.RUNNING) {
				return Optional.empty();
			}

			return Optional.of(supplier.get());
		}
	}

	private void runIfStateIs(State expectedState, Runnable action) {
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

	// ------------------------------------------------------------
	// Internal classes
	// ------------------------------------------------------------

	interface DispatcherServiceFactory {
		DispatcherService create(Collection<JobGraph> recoveredJobs, JobGraphWriter jobGraphWriter);
	}

	interface DispatcherService extends AutoCloseableAsync {
		DispatcherGateway getGateway();
	}

	enum State {
		CREATED,
		RUNNING,
		STOPPED
	}
}
