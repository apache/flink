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

package org.apache.flink.queryablestate.client.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.messages.KvStateRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.AbstractServerBase;
import org.apache.flink.queryablestate.network.AbstractServerHandler;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.KvStateLocationOracle;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The default implementation of the {@link KvStateClientProxy}.
 */
@Internal
public class KvStateClientProxyImpl extends AbstractServerBase<KvStateRequest, KvStateResponse> implements KvStateClientProxy {

	/** Number of threads used to process incoming requests. */
	private final int queryExecutorThreads;

	/** Statistics collector. */
	private final KvStateRequestStats stats;

	private final ConcurrentHashMap<JobID, KvStateLocationOracle> kvStateLocationOracles;

	/**
	 * Creates the Queryable State Client Proxy.
	 *
	 * <p>The server is instantiated using reflection by the
	 * {@link org.apache.flink.runtime.query.QueryableStateUtils#createKvStateClientProxy(InetAddress, Iterator, int, int, KvStateRequestStats)
	 * QueryableStateUtils.createKvStateClientProxy(InetAddress, Iterator, int, int, KvStateRequestStats)}.
	 *
	 * <p>The server needs to be started via {@link #start()} in order to bind
	 * to the configured bind address.
	 *
	 * @param bindAddress the address to listen to.
	 * @param bindPortIterator the port range to try to bind to.
	 * @param numEventLoopThreads number of event loop threads.
	 * @param numQueryThreads number of query threads.
	 * @param stats the statistics collector.
	 */
	public KvStateClientProxyImpl(
			final InetAddress bindAddress,
			final Iterator<Integer> bindPortIterator,
			final Integer numEventLoopThreads,
			final Integer numQueryThreads,
			final KvStateRequestStats stats) {

		super("Queryable State Proxy Server", bindAddress, bindPortIterator, numEventLoopThreads, numQueryThreads);
		Preconditions.checkArgument(numQueryThreads >= 1, "Non-positive number of query threads.");
		this.queryExecutorThreads = numQueryThreads;
		this.stats = Preconditions.checkNotNull(stats);

		this.kvStateLocationOracles = new ConcurrentHashMap<>(4);
	}

	@Override
	public InetSocketAddress getServerAddress() {
		return super.getServerAddress();
	}

	@Override
	public void start() throws Throwable {
		super.start();
	}

	@Override
	public void shutdown() {
		try {
			shutdownServer().get(10L, TimeUnit.SECONDS);
			log.info("{} was shutdown successfully.", getServerName());
		} catch (Exception e) {
			log.warn("{} shutdown failed: {}", getServerName(), e);
		}
	}

	@Override
	public void updateKvStateLocationOracle(JobID jobId, @Nullable KvStateLocationOracle kvStateLocationOracle) {
		if (kvStateLocationOracle == null) {
			kvStateLocationOracles.remove(jobId);
		} else {
			kvStateLocationOracles.put(jobId, kvStateLocationOracle);
		}
	}

	@Nullable
	@Override
	public KvStateLocationOracle getKvStateLocationOracle(JobID jobId) {
		final KvStateLocationOracle legacyKvStateLocationOracle = kvStateLocationOracles.get(HighAvailabilityServices.DEFAULT_JOB_ID);

		// we give preference to the oracle registered under the default job id
		// to make it work with the legacy code paths
		if (legacyKvStateLocationOracle != null) {
			return legacyKvStateLocationOracle;
		} else {
			return kvStateLocationOracles.get(jobId);
		}
	}

	@Override
	public AbstractServerHandler<KvStateRequest, KvStateResponse> initializeHandler() {
		MessageSerializer<KvStateRequest, KvStateResponse> serializer =
				new MessageSerializer<>(
						new KvStateRequest.KvStateRequestDeserializer(),
						new KvStateResponse.KvStateResponseDeserializer());
		return new KvStateClientProxyHandler(this, queryExecutorThreads, serializer, stats);
	}
}
