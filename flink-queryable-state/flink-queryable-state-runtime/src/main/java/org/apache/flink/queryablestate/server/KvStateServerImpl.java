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

package org.apache.flink.queryablestate.server;

import org.apache.flink.annotation.Internal;
import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.AbstractServerBase;
import org.apache.flink.queryablestate.network.AbstractServerHandler;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServer;
import org.apache.flink.util.Preconditions;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * The default implementation of the {@link KvStateServer}.
 */
@Internal
public class KvStateServerImpl extends AbstractServerBase<KvStateInternalRequest, KvStateResponse> implements KvStateServer {

	/** The {@link KvStateRegistry} to query for state instances. */
	private final KvStateRegistry kvStateRegistry;

	private final KvStateRequestStats stats;

	private MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer;

	/**
	 * Creates the state server.
	 *
	 * <p>The server is instantiated using reflection by the
	 * {@link org.apache.flink.runtime.query.QueryableStateUtils#createKvStateServer(InetAddress, Iterator, int, int, KvStateRegistry, KvStateRequestStats)
	 * QueryableStateUtils.createKvStateServer(InetAddress, Iterator, int, int, KvStateRegistry, KvStateRequestStats)}.
	 *
	 * <p>The server needs to be started via {@link #start()} in order to bind
	 * to the configured bind address.
	 *
	 * @param bindAddress the address to listen to.
	 * @param bindPortIterator the port range to try to bind to.
	 * @param numEventLoopThreads number of event loop threads.
	 * @param numQueryThreads number of query threads.
	 * @param kvStateRegistry {@link KvStateRegistry} to query for state instances.
	 * @param stats the statistics collector.
	 */
	public KvStateServerImpl(
			final InetAddress bindAddress,
			final Iterator<Integer> bindPortIterator,
			final Integer numEventLoopThreads,
			final Integer numQueryThreads,
			final KvStateRegistry kvStateRegistry,
			final KvStateRequestStats stats) {

		super("Queryable State Server", bindAddress, bindPortIterator, numEventLoopThreads, numQueryThreads);
		this.stats = Preconditions.checkNotNull(stats);
		this.kvStateRegistry = Preconditions.checkNotNull(kvStateRegistry);
	}

	@Override
	public AbstractServerHandler<KvStateInternalRequest, KvStateResponse> initializeHandler() {
		this.serializer = new MessageSerializer<>(
				new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
				new KvStateResponse.KvStateResponseDeserializer());
		return new KvStateServerHandler(this, kvStateRegistry, serializer, stats);
	}

	public MessageSerializer<KvStateInternalRequest, KvStateResponse> getSerializer() {
		Preconditions.checkState(serializer != null, "Server " + getServerName() + " has not been started.");
		return serializer;
	}

	@Override
	public void start() throws Throwable {
		super.start();
	}

	@Override
	public InetSocketAddress getServerAddress() {
		return super.getServerAddress();
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
}
