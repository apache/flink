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

package org.apache.flink.queryablestate.client;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.queryablestate.messages.KvStateRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.Client;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.query.netty.DisabledKvStateRequestStats;
import org.apache.flink.runtime.query.netty.message.KvStateSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceTypeInfo;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

/**
 * Client for querying Flink's managed state.
 *
 * <p>You can mark state as queryable via {@link StateDescriptor#setQueryable(String)}.
 * The state instance created from this descriptor will be published for queries when it's
 * created on the Task Managers and the location will be reported to the Job Manager.
 *
 * <p>The client connects to a {@link org.apache.flink.runtime.query.KvStateClientProxy Client Proxy}
 * running on a given Task Manager. The proxy is the entry point of the client to the Flink cluster.
 * It forwards the requests of the client to the Job Manager and the required Task Manager, and forwards
 * the final response back the client.
 *
 * <p>The proxy, initially resolves the location of the requested KvState via the JobManager. Resolved
 * locations are cached. When the server address of the requested KvState instance is determined, the
 * client sends out a request to the server. The returned final answer is then forwarded to the Client.
 */
@PublicEvolving
public class QueryableStateClient {

	private static final Logger LOG = LoggerFactory.getLogger(QueryableStateClient.class);

	/** The client that forwards the requests to the proxy. */
	private final Client<KvStateRequest, KvStateResponse> client;

	/** The address of the proxy this client is connected to. */
	private final KvStateServerAddress remoteAddress;

	/** The execution configuration used to instantiate the different (de-)serializers. */
	private ExecutionConfig executionConfig;

	/**
	 * Create the Queryable State Client.
	 * @param remoteHostname the hostname of the {@link org.apache.flink.runtime.query.KvStateClientProxy proxy}
	 *                       to connect to.
	 * @param remotePort the port of the proxy to connect to.
	 */
	public QueryableStateClient(final String remoteHostname, final int remotePort) throws UnknownHostException {
		this(InetAddress.getByName(Preconditions.checkNotNull(remoteHostname)), remotePort);
	}

	/**
	 * Create the Queryable State Client.
	 * @param remoteAddress the {@link InetAddress address} of the
	 *                      {@link org.apache.flink.runtime.query.KvStateClientProxy proxy} to connect to.
	 * @param remotePort the port of the proxy to connect to.
	 */
	public QueryableStateClient(final InetAddress remoteAddress, final int remotePort) {
		Preconditions.checkArgument(remotePort >= 0 && remotePort <= 65536,
				"Remote Port " + remotePort + " is out of valid port range (0-65536).");

		this.remoteAddress = new KvStateServerAddress(remoteAddress, remotePort);

		final MessageSerializer<KvStateRequest, KvStateResponse> messageSerializer =
				new MessageSerializer<>(
						new KvStateRequest.KvStateRequestDeserializer(),
						new KvStateResponse.KvStateResponseDeserializer());

		this.client = new Client<>(
				"Queryable State Client",
				Hardware.getNumberCPUCores(),
				messageSerializer,
				new DisabledKvStateRequestStats());
	}

	/** Shuts down the client. */
	public void shutdown() {
		client.shutdown();
	}

	/**
	 * Gets the {@link ExecutionConfig}.
	 */
	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	/**
	 * Replaces the existing {@link ExecutionConfig} (possibly {@code null}), with the provided one.
	 * @param config The new {@code configuration}.
	 * @return The old configuration, or {@code null} if none was specified.
	 * */
	public ExecutionConfig setExecutionConfig(ExecutionConfig config) {
		ExecutionConfig prev = executionConfig;
		this.executionConfig = config;
		return prev;
	}

	/**
	 * Returns a future holding the request result.	 *
	 * @param jobId                     JobID of the job the queryable state belongs to.
	 * @param queryableStateName        Name under which the state is queryable.
	 * @param key			            The key we are interested in.
	 * @param keyTypeHint				A {@link TypeHint} used to extract the type of the key.
	 * @param stateDescriptor			The {@link StateDescriptor} of the state we want to query.
	 * @return Future holding the result.
	 */
	@PublicEvolving
	public <K, V> CompletableFuture<V> getKvState(
			final JobID jobId,
			final String queryableStateName,
			final K key,
			final TypeHint<K> keyTypeHint,
			final StateDescriptor<?, V> stateDescriptor) {

		Preconditions.checkNotNull(keyTypeHint);

		TypeInformation<K> keyTypeInfo = keyTypeHint.getTypeInfo();
		return getKvState(jobId, queryableStateName, key, keyTypeInfo, stateDescriptor);
	}

	/**
	 * Returns a future holding the request result.	 *
	 * @param jobId                     JobID of the job the queryable state belongs to.
	 * @param queryableStateName        Name under which the state is queryable.
	 * @param key			            The key we are interested in.
	 * @param keyTypeInfo				The {@link TypeInformation} of the key.
	 * @param stateDescriptor			The {@link StateDescriptor} of the state we want to query.
	 * @return Future holding the result.
	 */
	@PublicEvolving
	public <K, V> CompletableFuture<V> getKvState(
			final JobID jobId,
			final String queryableStateName,
			final K key,
			final TypeInformation<K> keyTypeInfo,
			final StateDescriptor<?, V> stateDescriptor) {

		return getKvState(jobId, queryableStateName, key, VoidNamespace.INSTANCE,
				keyTypeInfo, VoidNamespaceTypeInfo.INSTANCE, stateDescriptor);
	}

	/**
	 * Returns a future holding the request result.
	 * @param jobId                     JobID of the job the queryable state belongs to.
	 * @param queryableStateName        Name under which the state is queryable.
	 * @param key			            The key that the state we request is associated with.
	 * @param namespace					The namespace of the state.
	 * @param keyTypeInfo				The {@link TypeInformation} of the keys.
	 * @param namespaceTypeInfo			The {@link TypeInformation} of the namespace.
	 * @param stateDescriptor			The {@link StateDescriptor} of the state we want to query.
	 * @return Future holding the result.
	 */
	@PublicEvolving
	public <K, V, N> CompletableFuture<V> getKvState(
			final JobID jobId,
			final String queryableStateName,
			final K key,
			final N namespace,
			final TypeInformation<K> keyTypeInfo,
			final TypeInformation<N> namespaceTypeInfo,
			final StateDescriptor<?, V> stateDescriptor) {

		Preconditions.checkNotNull(stateDescriptor);

		// initialize the value serializer based on the execution config.
		stateDescriptor.initializeSerializerUnlessSet(executionConfig);
		TypeSerializer<V> stateSerializer = stateDescriptor.getSerializer();

		return getKvState(jobId, queryableStateName, key,
				namespace, keyTypeInfo, namespaceTypeInfo, stateSerializer);
	}

	/**
	 * Returns a future holding the request result.
	 * @param jobId                     JobID of the job the queryable state belongs to.
	 * @param queryableStateName        Name under which the state is queryable.
	 * @param key			            The key that the state we request is associated with.
	 * @param namespace					The namespace of the state.
	 * @param keyTypeInfo				The {@link TypeInformation} of the keys.
	 * @param namespaceTypeInfo			The {@link TypeInformation} of the namespace.
	 * @param stateSerializer			The {@link TypeSerializer} of the state we want to query.
	 * @return Future holding the result.
	 */
	@PublicEvolving
	public <K, N, V> CompletableFuture<V> getKvState(
			final JobID jobId,
			final String queryableStateName,
			final K key,
			final N namespace,
			final TypeInformation<K> keyTypeInfo,
			final TypeInformation<N> namespaceTypeInfo,
			final TypeSerializer<V> stateSerializer) {

		Preconditions.checkNotNull(jobId);
		Preconditions.checkNotNull(queryableStateName);
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		Preconditions.checkNotNull(keyTypeInfo);
		Preconditions.checkNotNull(namespaceTypeInfo);
		Preconditions.checkNotNull(stateSerializer);

		TypeSerializer<K> keySerializer = keyTypeInfo.createSerializer(executionConfig);
		TypeSerializer<N> namespaceSerializer = namespaceTypeInfo.createSerializer(executionConfig);

		final byte[] serializedKeyAndNamespace;
		try {
			serializedKeyAndNamespace = KvStateSerializer
					.serializeKeyAndNamespace(key, keySerializer, namespace, namespaceSerializer);
		} catch (IOException e) {
			return FutureUtils.getFailedFuture(e);
		}

		return getKvState(jobId, queryableStateName, key.hashCode(), serializedKeyAndNamespace).thenApply(
				stateResponse -> {
					try {
						return KvStateSerializer.deserializeValue(stateResponse.getContent(), stateSerializer);
					} catch (IOException e) {
						throw new FlinkRuntimeException(e);
					}
				});
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
	 * @return Future holding the serialized result
	 */
	private CompletableFuture<KvStateResponse> getKvState(
			final JobID jobId,
			final String queryableStateName,
			final int keyHashCode,
			final byte[] serializedKeyAndNamespace) {
		LOG.info("Sending State Request to {}.", remoteAddress);
		try {
			KvStateRequest request = new KvStateRequest(jobId, queryableStateName, keyHashCode, serializedKeyAndNamespace);
			return client.sendRequest(remoteAddress, request);
		} catch (Exception e) {
			LOG.error("Unable to send KVStateRequest: ", e);
			return FutureUtils.getFailedFuture(e);
		}
	}
}
