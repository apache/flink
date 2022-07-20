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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateIdException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateKeyGroupLocationException;
import org.apache.flink.queryablestate.exceptions.UnknownLocationException;
import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.AbstractServerHandler;
import org.apache.flink.queryablestate.network.Client;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.queryablestate.server.KvStateServerImpl;
import org.apache.flink.runtime.jobmaster.KvStateLocationOracle;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.query.KvStateClientProxy;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * This handler acts as an internal (to the Flink cluster) client that receives the requests from
 * external clients, executes them by contacting the Job Manager (if necessary) and the Task Manager
 * holding the requested state, and forwards the answer back to the client.
 */
@Internal
@ChannelHandler.Sharable
public class KvStateClientProxyHandler
        extends AbstractServerHandler<KvStateRequest, KvStateResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(KvStateClientProxyHandler.class);

    /** The proxy using this handler. */
    private final KvStateClientProxy proxy;

    /** A cache to hold the location of different states for which we have already seen requests. */
    private final ConcurrentMap<Tuple2<JobID, String>, CompletableFuture<KvStateLocation>>
            lookupCache = new ConcurrentHashMap<>();

    /**
     * Network client to forward queries to {@link KvStateServerImpl state server} instances inside
     * the cluster.
     */
    private final Client<KvStateInternalRequest, KvStateResponse> kvStateClient;

    /**
     * Create the handler used by the {@link KvStateClientProxyImpl}.
     *
     * @param proxy the {@link KvStateClientProxyImpl proxy} using the handler.
     * @param queryExecutorThreads the number of threads used to process incoming requests.
     * @param serializer the {@link MessageSerializer} used to (de-) serialize the different
     *     messages.
     * @param stats server statistics collector.
     */
    public KvStateClientProxyHandler(
            final KvStateClientProxyImpl proxy,
            final int queryExecutorThreads,
            final MessageSerializer<KvStateRequest, KvStateResponse> serializer,
            final KvStateRequestStats stats) {

        super(proxy, serializer, stats);
        this.proxy = Preconditions.checkNotNull(proxy);
        this.kvStateClient = createInternalClient(queryExecutorThreads);
    }

    private static Client<KvStateInternalRequest, KvStateResponse> createInternalClient(
            int threads) {
        final MessageSerializer<KvStateInternalRequest, KvStateResponse> messageSerializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        return new Client<>(
                "Queryable State Proxy Client",
                threads,
                messageSerializer,
                new DisabledKvStateRequestStats());
    }

    @Override
    public CompletableFuture<KvStateResponse> handleRequest(
            final long requestId, final KvStateRequest request) {
        CompletableFuture<KvStateResponse> response = new CompletableFuture<>();
        executeActionAsync(response, request, false);
        return response;
    }

    private void executeActionAsync(
            final CompletableFuture<KvStateResponse> result,
            final KvStateRequest request,
            final boolean update) {

        if (!result.isDone()) {
            final CompletableFuture<KvStateResponse> operationFuture = getState(request, update);
            operationFuture.whenCompleteAsync(
                    (t, throwable) -> {
                        if (throwable != null) {
                            if (throwable.getCause() instanceof UnknownKvStateIdException
                                    || throwable.getCause()
                                            instanceof UnknownKvStateKeyGroupLocationException
                                    || throwable.getCause() instanceof ConnectException) {

                                // These failures are likely to be caused by out-of-sync
                                // KvStateLocation. Therefore we retry this query and
                                // force look up the location.

                                LOG.debug(
                                        "Retrying after failing to retrieve state due to: {}.",
                                        throwable.getCause().getMessage());
                                executeActionAsync(result, request, true);
                            } else {
                                result.completeExceptionally(throwable);
                            }
                        } else {
                            result.complete(t);
                        }
                    },
                    queryExecutor);

            result.whenComplete((t, throwable) -> operationFuture.cancel(false));
        }
    }

    private CompletableFuture<KvStateResponse> getState(
            final KvStateRequest request, final boolean forceUpdate) {

        return getKvStateLookupInfo(request.getJobId(), request.getStateName(), forceUpdate)
                .thenComposeAsync(
                        (Function<KvStateLocation, CompletableFuture<KvStateResponse>>)
                                location -> {
                                    final int keyGroupIndex =
                                            KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
                                                    request.getKeyHashCode(),
                                                    location.getNumKeyGroups());

                                    final InetSocketAddress serverAddress =
                                            location.getKvStateServerAddress(keyGroupIndex);
                                    if (serverAddress == null) {
                                        return FutureUtils.completedExceptionally(
                                                new UnknownKvStateKeyGroupLocationException(
                                                        getServerName()));
                                    } else {
                                        // Query server
                                        final KvStateID kvStateId =
                                                location.getKvStateID(keyGroupIndex);
                                        final KvStateInternalRequest internalRequest =
                                                new KvStateInternalRequest(
                                                        kvStateId,
                                                        request.getSerializedKeyAndNamespace());
                                        return kvStateClient.sendRequest(
                                                serverAddress, internalRequest);
                                    }
                                },
                        queryExecutor);
    }

    /**
     * Lookup the {@link KvStateLocation} for the given job and queryable state name.
     *
     * <p>The job manager will be queried for the location only if forced or no cached location can
     * be found. There are no guarantees about
     *
     * @param jobId JobID the state instance belongs to.
     * @param queryableStateName Name under which the state instance has been published.
     * @param forceUpdate Flag to indicate whether to force a update via the lookup service.
     * @return Future holding the KvStateLocation
     */
    private CompletableFuture<KvStateLocation> getKvStateLookupInfo(
            final JobID jobId, final String queryableStateName, final boolean forceUpdate) {

        final Tuple2<JobID, String> cacheKey = new Tuple2<>(jobId, queryableStateName);
        final CompletableFuture<KvStateLocation> cachedFuture = lookupCache.get(cacheKey);

        if (!forceUpdate && cachedFuture != null && !cachedFuture.isCompletedExceptionally()) {
            LOG.debug(
                    "Retrieving location for state={} of job={} from the cache.",
                    queryableStateName,
                    jobId);
            return cachedFuture;
        }

        final KvStateLocationOracle kvStateLocationOracle = proxy.getKvStateLocationOracle(jobId);

        if (kvStateLocationOracle != null) {
            LOG.debug(
                    "Retrieving location for state={} of job={} from the key-value state location oracle.",
                    queryableStateName,
                    jobId);
            final CompletableFuture<KvStateLocation> location = new CompletableFuture<>();
            lookupCache.put(cacheKey, location);

            kvStateLocationOracle
                    .requestKvStateLocation(jobId, queryableStateName)
                    .whenComplete(
                            (KvStateLocation kvStateLocation, Throwable throwable) -> {
                                if (throwable != null) {
                                    if (ExceptionUtils.stripCompletionException(throwable)
                                            instanceof FlinkJobNotFoundException) {
                                        // if the jobId was wrong, remove the entry from the cache.
                                        lookupCache.remove(cacheKey);
                                    }
                                    location.completeExceptionally(throwable);
                                } else {
                                    location.complete(kvStateLocation);
                                }
                            });

            return location;
        } else {
            return FutureUtils.completedExceptionally(
                    new UnknownLocationException(
                            "Could not retrieve location of state="
                                    + queryableStateName
                                    + " of job="
                                    + jobId
                                    + ". Potential reasons are: i) the state is not ready, or ii) the job does not exist."));
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return kvStateClient.shutdown();
    }
}
