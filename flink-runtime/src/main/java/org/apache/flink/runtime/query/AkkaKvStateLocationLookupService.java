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

package org.apache.flink.runtime.query;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.Recover;
import akka.pattern.Patterns;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Akka-based {@link KvStateLocationLookupService} that retrieves the current
 * JobManager address and uses it for lookups.
 */
class AkkaKvStateLocationLookupService implements KvStateLocationLookupService, LeaderRetrievalListener {

	private static final Logger LOG = LoggerFactory.getLogger(KvStateLocationLookupService.class);

	/** Future returned when no JobManager is available */
	private static final Future<ActorGateway> UNKNOWN_JOB_MANAGER = Futures.failed(new UnknownJobManager());

	/** Leader retrieval service to retrieve the current job manager. */
	private final LeaderRetrievalService leaderRetrievalService;

	/** The actor system used to resolve the JobManager address. */
	private final ActorSystem actorSystem;

	/** Timeout for JobManager ask-requests. */
	private final FiniteDuration askTimeout;

	/** Retry strategy factory on future failures. */
	private final LookupRetryStrategyFactory retryStrategyFactory;

	/** Current job manager future. */
	private volatile Future<ActorGateway> jobManagerFuture = UNKNOWN_JOB_MANAGER;

	/**
	 * Creates the Akka-based {@link KvStateLocationLookupService}.
	 *
	 * @param leaderRetrievalService Leader retrieval service to use.
	 * @param actorSystem            Actor system to use.
	 * @param askTimeout             Timeout for JobManager ask-requests.
	 * @param retryStrategyFactory   Retry strategy if no JobManager available.
	 */
	AkkaKvStateLocationLookupService(
			LeaderRetrievalService leaderRetrievalService,
			ActorSystem actorSystem,
			FiniteDuration askTimeout,
			LookupRetryStrategyFactory retryStrategyFactory) {

		this.leaderRetrievalService = Preconditions.checkNotNull(leaderRetrievalService, "Leader retrieval service");
		this.actorSystem = Preconditions.checkNotNull(actorSystem, "Actor system");
		this.askTimeout = Preconditions.checkNotNull(askTimeout, "Ask Timeout");
		this.retryStrategyFactory = Preconditions.checkNotNull(retryStrategyFactory, "Retry strategy factory");
	}

	public void start() {
		try {
			leaderRetrievalService.start(this);
		} catch (Exception e) {
			LOG.error("Failed to start leader retrieval service", e);
			throw new RuntimeException(e);
		}
	}

	public void shutDown() {
		try {
			leaderRetrievalService.stop();
		} catch (Exception e) {
			LOG.error("Failed to stop leader retrieval service", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public Future<KvStateLocation> getKvStateLookupInfo(final JobID jobId, final String registrationName) {
		return getKvStateLookupInfo(jobId, registrationName, retryStrategyFactory.createRetryStrategy());
	}

	/**
	 * Returns a future holding the {@link KvStateLocation} for the given job
	 * and KvState registration name.
	 *
	 * <p>If there is currently no JobManager registered with the service, the
	 * request is retried. The retry behaviour is specified by the
	 * {@link LookupRetryStrategy} of the lookup service.
	 *
	 * @param jobId               JobID the KvState instance belongs to
	 * @param registrationName    Name under which the KvState has been registered
	 * @param lookupRetryStrategy Retry strategy to use for retries on UnknownJobManager failures.
	 * @return Future holding the {@link KvStateLocation}
	 */
	@SuppressWarnings("unchecked")
	private Future<KvStateLocation> getKvStateLookupInfo(
			final JobID jobId,
			final String registrationName,
			final LookupRetryStrategy lookupRetryStrategy) {

		return jobManagerFuture
				.flatMap(new Mapper<ActorGateway, Future<Object>>() {
					@Override
					public Future<Object> apply(ActorGateway jobManager) {
						// Lookup the KvStateLocation
						Object msg = new KvStateMessage.LookupKvStateLocation(jobId, registrationName);
						return jobManager.ask(msg, askTimeout);
					}
				}, actorSystem.dispatcher())
				.mapTo(ClassTag$.MODULE$.<KvStateLocation>apply(KvStateLocation.class))
				.recoverWith(new Recover<Future<KvStateLocation>>() {
					@Override
					public Future<KvStateLocation> recover(Throwable failure) throws Throwable {
						// If the Future fails with UnknownJobManager, retry
						// the request. Otherwise all Futures will be failed
						// during the start up phase, when the JobManager did
						// not notify this service yet or leadership is lost
						// intermittently.
						if (failure instanceof UnknownJobManager && lookupRetryStrategy.tryRetry()) {
							return Patterns.after(
									lookupRetryStrategy.getRetryDelay(),
									actorSystem.scheduler(),
									actorSystem.dispatcher(),
									new Callable<Future<KvStateLocation>>() {
										@Override
										public Future<KvStateLocation> call() throws Exception {
											return getKvStateLookupInfo(
													jobId,
													registrationName,
													lookupRetryStrategy);
										}
									});
						} else {
							return Futures.failed(failure);
						}
					}
				}, actorSystem.dispatcher());
	}

	@Override
	public void notifyLeaderAddress(String leaderAddress, final UUID leaderSessionID) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received leader address notification {}:{}", leaderAddress, leaderSessionID);
		}

		if (leaderAddress == null) {
			jobManagerFuture = UNKNOWN_JOB_MANAGER;
		} else {
			jobManagerFuture = AkkaUtils.getActorRefFuture(leaderAddress, actorSystem, askTimeout)
					.map(new Mapper<ActorRef, ActorGateway>() {
						@Override
						public ActorGateway apply(ActorRef actorRef) {
							return new AkkaActorGateway(actorRef, leaderSessionID);
						}
					}, actorSystem.dispatcher());
		}
	}

	@Override
	public void handleError(Exception exception) {
		jobManagerFuture = Futures.failed(exception);
	}

	// ------------------------------------------------------------------------

	/**
	 * Retry strategy for failed lookups.
	 *
	 * <p>Usage:
	 * <pre>
	 * LookupRetryStrategy retryStrategy = LookupRetryStrategyFactory.create();
	 *
	 * if (retryStrategy.tryRetry()) {
	 *     // OK to retry
	 *     FiniteDuration retryDelay = retryStrategy.getRetryDelay();
	 * }
	 * </pre>
	 */
	interface LookupRetryStrategy {

		/**
		 * Returns the current retry.
		 *
		 * @return Current retry delay.
		 */
		FiniteDuration getRetryDelay();

		/**
		 * Tries another retry and returns whether it is allowed or not.
		 *
		 * @return Whether it is allowed to do another restart or not.
		 */
		boolean tryRetry();

	}

	/**
	 * Factory for retry strategies.
	 */
	interface LookupRetryStrategyFactory {

		/**
		 * Creates a new retry strategy.
		 *
		 * @return The retry strategy.
		 */
		LookupRetryStrategy createRetryStrategy();

	}

	/**
	 * Factory for disabled retries.
	 */
	static class DisabledLookupRetryStrategyFactory implements LookupRetryStrategyFactory {

		private static final DisabledLookupRetryStrategy RETRY_STRATEGY = new DisabledLookupRetryStrategy();

		@Override
		public LookupRetryStrategy createRetryStrategy() {
			return RETRY_STRATEGY;
		}

		private static class DisabledLookupRetryStrategy implements LookupRetryStrategy {

			@Override
			public FiniteDuration getRetryDelay() {
				return FiniteDuration.Zero();
			}

			@Override
			public boolean tryRetry() {
				return false;
			}
		}

	}

	/**
	 * Factory for fixed delay retries.
	 */
	static class FixedDelayLookupRetryStrategyFactory implements LookupRetryStrategyFactory {

		private final int maxRetries;
		private final FiniteDuration retryDelay;

		FixedDelayLookupRetryStrategyFactory(int maxRetries, FiniteDuration retryDelay) {
			this.maxRetries = maxRetries;
			this.retryDelay = retryDelay;
		}

		@Override
		public LookupRetryStrategy createRetryStrategy() {
			return new FixedDelayLookupRetryStrategy(maxRetries, retryDelay);
		}

		private static class FixedDelayLookupRetryStrategy implements LookupRetryStrategy {

			private final Object retryLock = new Object();
			private final int maxRetries;
			private final FiniteDuration retryDelay;
			private int numRetries;

			public FixedDelayLookupRetryStrategy(int maxRetries, FiniteDuration retryDelay) {
				Preconditions.checkArgument(maxRetries >= 0, "Negative number maximum retries");
				this.maxRetries = maxRetries;
				this.retryDelay = Preconditions.checkNotNull(retryDelay, "Retry delay");
			}

			@Override
			public FiniteDuration getRetryDelay() {
				synchronized (retryLock) {
					return retryDelay;
				}
			}

			@Override
			public boolean tryRetry() {
				synchronized (retryLock) {
					if (numRetries < maxRetries) {
						numRetries++;
						return true;
					} else {
						return false;
					}
				}
			}
		}
	}
}
