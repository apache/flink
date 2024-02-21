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

package org.apache.flink.runtime.rpc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/** Utility functions for Flink's RPC implementation. */
public class RpcUtils {

    /**
     * <b>HACK:</b> Set to 21474835 seconds, Pekko's maximum delay (Akka 2.4.20). The value cannot
     * be higher or an {@link IllegalArgumentException} will be thrown during an RPC. Check the
     * private method {@code checkMaxDelay()} in {@link
     * org.apache.pekko.actor.LightArrayRevolverScheduler}.
     */
    public static final Time INF_TIMEOUT = Time.seconds(21474835);

    public static final Duration INF_DURATION = Duration.ofSeconds(21474835);

    /**
     * Extracts all {@link RpcGateway} interfaces implemented by the given clazz.
     *
     * @param clazz from which to extract the implemented RpcGateway interfaces
     * @return A set of all implemented RpcGateway interfaces
     */
    public static Set<Class<? extends RpcGateway>> extractImplementedRpcGateways(Class<?> clazz) {
        HashSet<Class<? extends RpcGateway>> interfaces = new HashSet<>();

        while (clazz != null) {
            for (Class<?> interfaze : clazz.getInterfaces()) {
                if (RpcGateway.class.isAssignableFrom(interfaze)) {
                    interfaces.add((Class<? extends RpcGateway>) interfaze);
                }
            }

            clazz = clazz.getSuperclass();
        }

        return interfaces;
    }

    /**
     * Shuts the given {@link RpcEndpoint}s down and awaits their termination.
     *
     * @param rpcEndpoints to terminate
     * @throws ExecutionException if a problem occurred
     * @throws InterruptedException if the operation has been interrupted
     */
    @VisibleForTesting
    public static void terminateRpcEndpoint(RpcEndpoint... rpcEndpoints)
            throws ExecutionException, InterruptedException {
        terminateAsyncCloseables(Arrays.asList(rpcEndpoints));
    }

    /**
     * Shuts the given rpc services down and waits for their termination.
     *
     * @param rpcServices to shut down
     * @throws InterruptedException if the operation has been interrupted
     * @throws ExecutionException if a problem occurred
     */
    @VisibleForTesting
    public static void terminateRpcService(RpcService... rpcServices)
            throws InterruptedException, ExecutionException {
        terminateAsyncCloseables(
                Arrays.stream(rpcServices)
                        .map(rpcService -> (AutoCloseableAsync) rpcService::closeAsync)
                        .collect(Collectors.toList()));
    }

    private static void terminateAsyncCloseables(
            Collection<? extends AutoCloseableAsync> closeables)
            throws InterruptedException, ExecutionException {
        final Collection<CompletableFuture<?>> terminationFutures =
                new ArrayList<>(closeables.size());

        for (AutoCloseableAsync closeableAsync : closeables) {
            if (closeableAsync != null) {
                terminationFutures.add(closeableAsync.closeAsync());
            }
        }

        FutureUtils.waitForAll(terminationFutures).get();
    }

    /**
     * Returns the hostname onto which the given {@link RpcService} has been bound. If the {@link
     * RpcService} has been started in local mode, then the hostname is {@code "hostname"}.
     *
     * @param rpcService to retrieve the hostname for
     * @return hostname onto which the given {@link RpcService} has been bound or localhost
     */
    public static String getHostname(RpcService rpcService) {
        final String rpcServiceAddress = rpcService.getAddress();
        return rpcServiceAddress != null && rpcServiceAddress.isEmpty()
                ? "localhost"
                : rpcServiceAddress;
    }

    public static RpcSystem.ForkJoinExecutorConfiguration getTestForkJoinExecutorConfiguration() {
        return new RpcSystem.ForkJoinExecutorConfiguration(1.0, 2, 4);
    }

    /**
     * Convenient shortcut for constructing a remote RPC Service that takes care of checking for
     * null and empty optionals.
     *
     * @see RpcSystem#remoteServiceBuilder(Configuration, String, String)
     */
    public static RpcService createRemoteRpcService(
            RpcSystem rpcSystem,
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange,
            @Nullable String bindAddress,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort)
            throws Exception {
        RpcSystem.RpcServiceBuilder rpcServiceBuilder =
                rpcSystem.remoteServiceBuilder(configuration, externalAddress, externalPortRange);
        if (bindAddress != null) {
            rpcServiceBuilder = rpcServiceBuilder.withBindAddress(bindAddress);
        }
        if (bindPort.isPresent()) {
            rpcServiceBuilder = rpcServiceBuilder.withBindPort(bindPort.get());
        }
        return rpcServiceBuilder.createAndStart();
    }

    // We don't want this class to be instantiable
    private RpcUtils() {}
}
