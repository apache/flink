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

import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.Recover;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.query.AkkaKvStateLocationLookupService.FixedDelayLookupRetryStrategyFactory;
import org.apache.flink.runtime.query.AkkaKvStateLocationLookupService.LookupRetryStrategyFactory;
import org.apache.flink.runtime.query.netty.DisabledKvStateRequestStats;
import org.apache.flink.runtime.query.netty.KvStateClient;
import org.apache.flink.runtime.query.netty.KvStateServer;
import org.apache.flink.runtime.query.netty.UnknownKeyOrNamespace;
import org.apache.flink.runtime.query.netty.UnknownKvStateID;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.net.ConnectException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client for queryable state.
 *
 * <p>You can mark state as queryable via {@link StateDescriptor#setQueryable(String)}.
 * The state instance created from this descriptor will be published for queries
 * when it's created on the TaskManagers and the location will be reported to
 * the JobManager.
 *
 * <p>The client resolves the location of the requested KvState via the
 * JobManager. Resolved locations are cached. When the server address of the
 * requested KvState instance is determined, the client sends out a request to
 * the server.
 */
public class QueryableStateClient {

	private static final Logger LOG = LoggerFactory.getLogger(QueryableStateClient.class);

	/**
	 * {@link KvStateLocation} lookup to resolve the address of KvState instances.
	 */
	private final KvStateLocationLookupService lookupService;

	/**
	 * Network client for queries against {@link KvStateServer} instances.
	 */
	private final KvStateClient kvStateClient;

	/**
	 * Execution context.
	 */
	private final ExecutionContext executionContext;

	/**
	 * Cache for {@link KvStateLocation} instances keyed by job and name.
	 */
	private final ConcurrentMap<Tuple2<JobID, String>, Future<KvStateLocation>> lookupCache =
			new ConcurrentHashMap<>();

	/** This is != null, iff we started the actor system. */
	private final ActorSystem actorSystem;

	/**
	 * Creates a client from the given configuration.
	 *
	 * <p>This will create multiple Thread pools: one for the started actor
	 * system and another for the network client.
	 *
	 * @param config Configuration to use.
	 * @throws Exception Failures are forwarded
	 */
	public QueryableStateClient(Configuration config) throws Exception {
		Preconditions.checkNotNull(config, "Configuration");

		// Create a leader retrieval service
		LeaderRetrievalService leaderRetrievalService = LeaderRetrievalUtils
				.createLeaderRetrievalService(config);

		// Get the ask timeout
		String askTimeoutString = config.getString(
				ConfigConstants.AKKA_ASK_TIMEOUT,
				ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);

		Duration timeout = FiniteDuration.apply(askTimeoutString);
		if (!timeout.isFinite()) {
			throw new IllegalConfigurationException(ConfigConstants.AKKA_ASK_TIMEOUT
					+ " is not a finite timeout ('" + askTimeoutString + "')");
		}

		FiniteDuration askTimeout = (FiniteDuration) timeout;

		int lookupRetries = config.getInteger(QueryableStateOptions.CLIENT_LOOKUP_RETRIES);
		int lookupRetryDelayMillis = config.getInteger(QueryableStateOptions.CLIENT_LOOKUP_RETRY_DELAY);

		// Retries if no JobManager is around
		LookupRetryStrategyFactory retryStrategy = new FixedDelayLookupRetryStrategyFactory(
				lookupRetries,
				FiniteDuration.apply(lookupRetryDelayMillis, "ms"));

		// Create the actor system
		@SuppressWarnings("unchecked")
		Option<Tuple2<String, Object>> remoting = new Some(new Tuple2<>("", 0));
		this.actorSystem = AkkaUtils.createActorSystem(config, remoting);

		AkkaKvStateLocationLookupService lookupService = new AkkaKvStateLocationLookupService(
				leaderRetrievalService,
				actorSystem,
				askTimeout,
				retryStrategy);

		int numEventLoopThreads = config.getInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS);

		if (numEventLoopThreads == 0) {
			numEventLoopThreads = Runtime.getRuntime().availableProcessors();
		}

		// Create the network client
		KvStateClient networkClient = new KvStateClient(
				numEventLoopThreads,
				new DisabledKvStateRequestStats());

		this.lookupService = lookupService;
		this.kvStateClient = networkClient;
		this.executionContext = actorSystem.dispatcher();

		this.lookupService.start();
	}

	/**
	 * Creates a client.
	 *
	 * @param lookupService    Location lookup service
	 * @param kvStateClient    Network client for queries
	 * @param executionContext Execution context for futures
	 */
	public QueryableStateClient(
			KvStateLocationLookupService lookupService,
			KvStateClient kvStateClient,
			ExecutionContext executionContext) {

		this.lookupService = Preconditions.checkNotNull(lookupService, "KvStateLocationLookupService");
		this.kvStateClient = Preconditions.checkNotNull(kvStateClient, "KvStateClient");
		this.executionContext = Preconditions.checkNotNull(executionContext, "ExecutionContext");
		this.actorSystem = null;

		this.lookupService.start();
	}

	/**
	 * Returns the execution context of this client.
	 *
	 * @return The execution context used by the client.
	 */
	public ExecutionContext getExecutionContext() {
		return executionContext;
	}

	/**
	 * Shuts down the client and all components.
	 */
	public void shutDown() {
		try {
			lookupService.shutDown();
		} catch (Throwable t) {
			LOG.error("Failed to shut down KvStateLookupService", t);
		}

		try {
			kvStateClient.shutDown();
		} catch (Throwable t) {
			LOG.error("Failed to shut down KvStateClient", t);
		}

		if (actorSystem != null) {
			try {
				actorSystem.shutdown();
			} catch (Throwable t) {
				LOG.error("Failed to shut down ActorSystem");
			}
		}
	}

	/**
	 * Returns a future holding the serialized request result.
	 *
	 * <p>If the server does not serve a KvState instance with the given ID,
	 * the Future will be failed with a {@link UnknownKvStateID}.
	 *
	 * <p>If the KvState instance does not hold any data for the given key
	 * and namespace, the Future will be failed with a {@link UnknownKeyOrNamespace}.
	 *
	 * <p>All other failures are forwarded to the Future.
	 *
	 * @param jobId                     JobID of the job the queryable state
	 *                                  belongs to
	 * @param queryableStateName        Name under which the state is queryable
	 * @param keyHashCode               Integer hash code of the key (result of
	 *                                  a call to {@link Object#hashCode()}
	 * @param serializedKeyAndNamespace Serialized key and namespace to query
	 *                                  KvState instance with
	 * @return Future holding the serialized result
	 */
	@SuppressWarnings("unchecked")
	public Future<byte[]> getKvState(
			final JobID jobId,
			final String queryableStateName,
			final int keyHashCode,
			final byte[] serializedKeyAndNamespace) {

		return getKvState(jobId, queryableStateName, keyHashCode, serializedKeyAndNamespace, false)
				.recoverWith(new Recover<Future<byte[]>>() {
					@Override
					public Future<byte[]> recover(Throwable failure) throws Throwable {
						if (failure instanceof UnknownKvStateID ||
								failure instanceof UnknownKvStateKeyGroupLocation ||
								failure instanceof UnknownKvStateLocation ||
								failure instanceof ConnectException) {
							// These failures are likely to be caused by out-of-sync
							// KvStateLocation. Therefore we retry this query and
							// force look up the location.
							return getKvState(
									jobId,
									queryableStateName,
									keyHashCode,
									serializedKeyAndNamespace,
									true);
						} else {
							return Futures.failed(failure);
						}
					}
				}, executionContext);
	}

	/**
	 * Returns a future holding the serialized request result.
	 *
	 * @param jobId                     JobID of the job the queryable state
	 *                                  belongs to
	 * @param queryableStateName        Name under which the state is queryable
	 * @param keyHashCode               Integer hash code of the key (result of
	 *                                  a call to {@link Object#hashCode()}
	 * @param serializedKeyAndNamespace Serialized key and namespace to query
	 *                                  KvState instance with
	 * @param forceLookup               Flag to force lookup of the {@link KvStateLocation}
	 * @return Future holding the serialized result
	 */
	private Future<byte[]> getKvState(
			final JobID jobId,
			final String queryableStateName,
			final int keyHashCode,
			final byte[] serializedKeyAndNamespace,
			boolean forceLookup) {

		return getKvStateLookupInfo(jobId, queryableStateName, forceLookup)
				.flatMap(new Mapper<KvStateLocation, Future<byte[]>>() {
					@Override
					public Future<byte[]> apply(KvStateLocation lookup) {
						int keyGroupIndex = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(keyHashCode, lookup.getNumKeyGroups());

						KvStateServerAddress serverAddress = lookup.getKvStateServerAddress(keyGroupIndex);
						if (serverAddress == null) {
							return Futures.failed(new UnknownKvStateKeyGroupLocation());
						} else {
							// Query server
							KvStateID kvStateId = lookup.getKvStateID(keyGroupIndex);
							return kvStateClient.getKvState(serverAddress, kvStateId, serializedKeyAndNamespace);
						}
					}
				}, executionContext);
	}

	/**
	 * Lookup the {@link KvStateLocation} for the given job and queryable state
	 * name.
	 *
	 * <p>The job manager will be queried for the location only if forced or no
	 * cached location can be found. There are no guarantees about
	 *
	 * @param jobId              JobID the state instance belongs to.
	 * @param queryableStateName Name under which the state instance has been published.
	 * @param forceUpdate        Flag to indicate whether to force a update via the lookup service.
	 * @return Future holding the KvStateLocation
	 */
	private Future<KvStateLocation> getKvStateLookupInfo(
			JobID jobId,
			final String queryableStateName,
			boolean forceUpdate) {

		if (forceUpdate) {
			Future<KvStateLocation> lookupFuture = lookupService
					.getKvStateLookupInfo(jobId, queryableStateName);
			lookupCache.put(new Tuple2<>(jobId, queryableStateName), lookupFuture);
			return lookupFuture;
		} else {
			Tuple2<JobID, String> cacheKey = new Tuple2<>(jobId, queryableStateName);
			final Future<KvStateLocation> cachedFuture = lookupCache.get(cacheKey);

			if (cachedFuture == null) {
				Future<KvStateLocation> lookupFuture = lookupService
						.getKvStateLookupInfo(jobId, queryableStateName);

				Future<KvStateLocation> previous = lookupCache.putIfAbsent(cacheKey, lookupFuture);
				if (previous == null) {
					return lookupFuture;
				} else {
					return previous;
				}
			} else {
				return cachedFuture;
			}
		}
	}

}
