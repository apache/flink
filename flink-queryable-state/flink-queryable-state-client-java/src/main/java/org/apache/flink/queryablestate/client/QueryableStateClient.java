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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.ImmutableAggregatingState;
import org.apache.flink.queryablestate.client.state.ImmutableListState;
import org.apache.flink.queryablestate.client.state.ImmutableMapState;
import org.apache.flink.queryablestate.client.state.ImmutableReducingState;
import org.apache.flink.queryablestate.client.state.ImmutableValueState;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.queryablestate.messages.KvStateRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.Client;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.LambdaUtil;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Client for querying Flink's managed state.
 *
 * <p>You can mark state as queryable via {@link StateDescriptor#setQueryable(String)}. The state
 * instance created from this descriptor will be published for queries when it's created on the Task
 * Managers and the location will be reported to the Job Manager.
 *
 * <p>The client connects to a {@code Client Proxy} running on a given Task Manager. The proxy is
 * the entry point of the client to the Flink cluster. It forwards the requests of the client to the
 * Job Manager and the required Task Manager, and forwards the final response back the client.
 *
 * <p>The proxy, initially resolves the location of the requested KvState via the JobManager.
 * Resolved locations are cached. When the server address of the requested KvState instance is
 * determined, the client sends out a request to the server. The returned final answer is then
 * forwarded to the Client.
 *
 * @deprecated The Queryable State feature is deprecated since Flink 1.18, and will be removed in a
 *     future Flink major version.
 */
@PublicEvolving
@Deprecated
public class QueryableStateClient {

    private static final Logger LOG = LoggerFactory.getLogger(QueryableStateClient.class);

    private static final Map<StateDescriptor.Type, StateFactory> STATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateFactory) ImmutableValueState::createState),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateFactory) ImmutableListState::createState),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateFactory) ImmutableMapState::createState),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateFactory) ImmutableAggregatingState::createState),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateFactory) ImmutableReducingState::createState))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private interface StateFactory {
        <T, S extends State> S createState(StateDescriptor<S, T> stateDesc, byte[] serializedState)
                throws Exception;
    }

    /** The client that forwards the requests to the proxy. */
    private final Client<KvStateRequest, KvStateResponse> client;

    /** The address of the proxy this client is connected to. */
    private final InetSocketAddress remoteAddress;

    /** The execution configuration used to instantiate the different (de-)serializers. */
    private ExecutionConfig executionConfig;

    /** The user code classloader, used in loading serializers. */
    private ClassLoader userClassLoader;

    /**
     * Create the Queryable State Client.
     *
     * @param remoteHostname the hostname of the {@code Client Proxy} to connect to.
     * @param remotePort the port of the proxy to connect to.
     */
    public QueryableStateClient(final String remoteHostname, final int remotePort)
            throws UnknownHostException {
        this(InetAddress.getByName(Preconditions.checkNotNull(remoteHostname)), remotePort);
    }

    /**
     * Create the Queryable State Client.
     *
     * @param remoteAddress the {@link InetAddress address} of the {@code Client Proxy} to connect
     *     to.
     * @param remotePort the port of the proxy to connect to.
     */
    public QueryableStateClient(final InetAddress remoteAddress, final int remotePort) {
        Preconditions.checkArgument(
                NetUtils.isValidHostPort(remotePort),
                "Remote Port " + remotePort + " is out of valid port range [0-65535].");

        this.remoteAddress = new InetSocketAddress(remoteAddress, remotePort);

        final MessageSerializer<KvStateRequest, KvStateResponse> messageSerializer =
                new MessageSerializer<>(
                        new KvStateRequest.KvStateRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        this.client =
                new Client<>(
                        "Queryable State Client",
                        1,
                        messageSerializer,
                        new DisabledKvStateRequestStats());
    }

    /**
     * Shuts down the client and returns a {@link CompletableFuture} that will be completed when the
     * shutdown process is completed.
     *
     * <p>If an exception is thrown for any reason, then the returned future will be completed
     * exceptionally with that exception.
     *
     * @return A {@link CompletableFuture} for further handling of the shutdown result.
     */
    public CompletableFuture<?> shutdownAndHandle() {
        return client.shutdown();
    }

    /**
     * Shuts down the client and waits until shutdown is completed.
     *
     * <p>If an exception is thrown, a warning is logged containing the exception message.
     */
    public void shutdownAndWait() {
        try {
            client.shutdown().get();
            LOG.info("The Queryable State Client was shutdown successfully.");
        } catch (Exception e) {
            LOG.warn("The Queryable State Client shutdown failed: ", e);
        }
    }

    /** Gets the {@link ExecutionConfig}. */
    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    /**
     * Replaces the existing {@link ExecutionConfig} (possibly {@code null}), with the provided one.
     *
     * @param config The new {@code configuration}.
     * @return The old configuration, or {@code null} if none was specified.
     */
    public ExecutionConfig setExecutionConfig(ExecutionConfig config) {
        ExecutionConfig prev = executionConfig;
        this.executionConfig = config;
        return prev;
    }

    /**
     * * Replaces the existing {@link ClassLoader} (possibly {@code null}), with the provided one.
     *
     * @param userClassLoader The new {@code userClassLoader}.
     * @return The old classloader, or {@code null} if none was specified.
     */
    public ClassLoader setUserClassLoader(ClassLoader userClassLoader) {
        ClassLoader prev = this.userClassLoader;
        this.userClassLoader = userClassLoader;
        return prev;
    }

    /**
     * Returns a future holding the request result.
     *
     * @param jobId JobID of the job the queryable state belongs to.
     * @param queryableStateName Name under which the state is queryable.
     * @param key The key we are interested in.
     * @param keyTypeHint A {@link TypeHint} used to extract the type of the key.
     * @param stateDescriptor The {@link StateDescriptor} of the state we want to query.
     * @return Future holding the immutable {@link State} object containing the result.
     */
    @PublicEvolving
    public <K, S extends State, V> CompletableFuture<S> getKvState(
            final JobID jobId,
            final String queryableStateName,
            final K key,
            final TypeHint<K> keyTypeHint,
            final StateDescriptor<S, V> stateDescriptor) {

        Preconditions.checkNotNull(keyTypeHint);

        TypeInformation<K> keyTypeInfo = keyTypeHint.getTypeInfo();
        return getKvState(jobId, queryableStateName, key, keyTypeInfo, stateDescriptor);
    }

    /**
     * Returns a future holding the request result.
     *
     * @param jobId JobID of the job the queryable state belongs to.
     * @param queryableStateName Name under which the state is queryable.
     * @param key The key we are interested in.
     * @param keyTypeInfo The {@link TypeInformation} of the key.
     * @param stateDescriptor The {@link StateDescriptor} of the state we want to query.
     * @return Future holding the immutable {@link State} object containing the result.
     */
    @PublicEvolving
    public <K, S extends State, V> CompletableFuture<S> getKvState(
            final JobID jobId,
            final String queryableStateName,
            final K key,
            final TypeInformation<K> keyTypeInfo,
            final StateDescriptor<S, V> stateDescriptor) {

        return getKvState(
                jobId,
                queryableStateName,
                key,
                VoidNamespace.INSTANCE,
                keyTypeInfo,
                VoidNamespaceTypeInfo.INSTANCE,
                stateDescriptor);
    }

    /**
     * Returns a future holding the request result.
     *
     * @param jobId JobID of the job the queryable state belongs to.
     * @param queryableStateName Name under which the state is queryable.
     * @param key The key that the state we request is associated with.
     * @param namespace The namespace of the state.
     * @param keyTypeInfo The {@link TypeInformation} of the keys.
     * @param namespaceTypeInfo The {@link TypeInformation} of the namespace.
     * @param stateDescriptor The {@link StateDescriptor} of the state we want to query.
     * @return Future holding the immutable {@link State} object containing the result.
     */
    private <K, N, S extends State, V> CompletableFuture<S> getKvState(
            final JobID jobId,
            final String queryableStateName,
            final K key,
            final N namespace,
            final TypeInformation<K> keyTypeInfo,
            final TypeInformation<N> namespaceTypeInfo,
            final StateDescriptor<S, V> stateDescriptor) {

        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(queryableStateName);
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(namespace);

        Preconditions.checkNotNull(keyTypeInfo);
        Preconditions.checkNotNull(namespaceTypeInfo);
        Preconditions.checkNotNull(stateDescriptor);

        TypeSerializer<K> keySerializer = keyTypeInfo.createSerializer(executionConfig);
        TypeSerializer<N> namespaceSerializer = namespaceTypeInfo.createSerializer(executionConfig);

        stateDescriptor.initializeSerializerUnlessSet(executionConfig);

        final byte[] serializedKeyAndNamespace;
        try {
            serializedKeyAndNamespace =
                    KvStateSerializer.serializeKeyAndNamespace(
                            key, keySerializer, namespace, namespaceSerializer);
        } catch (IOException e) {
            return FutureUtils.completedExceptionally(e);
        }

        ClassLoader classLoaderToUse =
                userClassLoader != null
                        ? userClassLoader
                        : Thread.currentThread().getContextClassLoader();
        return getKvState(jobId, queryableStateName, key.hashCode(), serializedKeyAndNamespace)
                .thenApply(
                        stateResponse ->
                                LambdaUtil.withContextClassLoader(
                                        classLoaderToUse,
                                        () -> createState(stateResponse, stateDescriptor)));
    }

    private <T, S extends State> S createState(
            KvStateResponse stateResponse, StateDescriptor<S, T> stateDescriptor) {
        StateFactory stateFactory = STATE_FACTORIES.get(stateDescriptor.getType());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDescriptor.getClass(), this.getClass());
            throw new FlinkRuntimeException(message);
        }
        try {
            return stateFactory.createState(stateDescriptor, stateResponse.getContent());
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    /**
     * Returns a future holding the serialized request result.
     *
     * @param jobId JobID of the job the queryable state belongs to
     * @param queryableStateName Name under which the state is queryable
     * @param keyHashCode Integer hash code of the key (result of a call to {@link
     *     Object#hashCode()}
     * @param serializedKeyAndNamespace Serialized key and namespace to query KvState instance with
     * @return Future holding the serialized result
     */
    private CompletableFuture<KvStateResponse> getKvState(
            final JobID jobId,
            final String queryableStateName,
            final int keyHashCode,
            final byte[] serializedKeyAndNamespace) {
        LOG.debug("Sending State Request to {}.", remoteAddress);
        try {
            KvStateRequest request =
                    new KvStateRequest(
                            jobId, queryableStateName, keyHashCode, serializedKeyAndNamespace);
            return client.sendRequest(remoteAddress, request);
        } catch (Exception e) {
            LOG.error("Unable to send KVStateRequest: ", e);
            return FutureUtils.completedExceptionally(e);
        }
    }
}
