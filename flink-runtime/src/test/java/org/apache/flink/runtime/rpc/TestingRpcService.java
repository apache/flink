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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An RPC Service implementation for testing. This RPC service acts as a replacement for the regular
 * RPC service for cases where tests need to return prepared mock gateways instead of proper RPC
 * gateways.
 *
 * <p>The TestingRpcService can be used for example in the following fashion, using <i>Mockito</i>
 * for mocks and verification:
 *
 * <pre>{@code
 * TestingRpcService rpc = new TestingRpcService();
 *
 * ResourceManagerGateway testGateway = mock(ResourceManagerGateway.class);
 * rpc.registerGateway("myAddress", testGateway);
 *
 * MyComponentToTest component = new MyComponentToTest();
 * component.triggerSomethingThatCallsTheGateway();
 *
 * verify(testGateway, timeout(1000)).theTestMethod(any(UUID.class), anyString());
 * }</pre>
 */
public class TestingRpcService implements RpcService {

    // load RpcSystem once to save initialization costs
    // this is safe because it is state-less
    private static final RpcSystem RPC_SYSTEM_SINGLETON = RpcSystem.load();

    private static final Function<RpcGateway, CompletableFuture<RpcGateway>>
            DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION = CompletableFuture::completedFuture;

    /** Map of pre-registered connections. */
    private final ConcurrentHashMap<String, RpcGateway> registeredConnections;

    private volatile Function<RpcGateway, CompletableFuture<RpcGateway>> rpcGatewayFutureFunction =
            DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION;

    private final RpcService backingRpcService;

    /** Creates a new {@code TestingRpcService}, using the given configuration. */
    public TestingRpcService() {
        try {
            this.backingRpcService =
                    RPC_SYSTEM_SINGLETON.localServiceBuilder(new Configuration()).createAndStart();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.registeredConnections = new ConcurrentHashMap<>();
    }

    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> stopService() {
        final CompletableFuture<Void> terminationFuture = backingRpcService.stopService();

        terminationFuture.whenComplete(
                (Void ignored, Throwable throwable) -> {
                    registeredConnections.clear();
                });

        return terminationFuture;
    }

    // ------------------------------------------------------------------------
    // connections
    // ------------------------------------------------------------------------

    public void registerGateway(String address, RpcGateway gateway) {
        checkNotNull(address);
        checkNotNull(gateway);

        if (registeredConnections.putIfAbsent(address, gateway) != null) {
            throw new IllegalStateException("a gateway is already registered under " + address);
        }
    }

    public void unregisterGateway(String address) {
        checkNotNull(address);
        if (registeredConnections.remove(address) == null) {
            throw new IllegalStateException("no gateway is registered under " + address);
        }
    }

    @SuppressWarnings("unchecked")
    private <C extends RpcGateway> CompletableFuture<C> getRpcGatewayFuture(C gateway) {
        return (CompletableFuture<C>) rpcGatewayFutureFunction.apply(gateway);
    }

    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
        RpcGateway gateway = registeredConnections.get(address);

        if (gateway != null) {
            if (clazz.isAssignableFrom(gateway.getClass())) {
                @SuppressWarnings("unchecked")
                C typedGateway = (C) gateway;
                return getRpcGatewayFuture(typedGateway);
            } else {
                return FutureUtils.completedExceptionally(
                        new Exception(
                                "Gateway registered under "
                                        + address
                                        + " is not of type "
                                        + clazz));
            }
        } else {
            return backingRpcService.connect(address, clazz);
        }
    }

    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        RpcGateway gateway = registeredConnections.get(address);

        if (gateway != null) {
            if (clazz.isAssignableFrom(gateway.getClass())) {
                @SuppressWarnings("unchecked")
                C typedGateway = (C) gateway;
                return getRpcGatewayFuture(typedGateway);
            } else {
                return FutureUtils.completedExceptionally(
                        new Exception(
                                "Gateway registered under "
                                        + address
                                        + " is not of type "
                                        + clazz));
            }
        } else {
            return backingRpcService.connect(address, fencingToken, clazz);
        }
    }

    public void clearGateways() {
        registeredConnections.clear();
    }

    public void resetRpcGatewayFutureFunction() {
        rpcGatewayFutureFunction = DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION;
    }

    public void setRpcGatewayFutureFunction(
            Function<RpcGateway, CompletableFuture<RpcGateway>> rpcGatewayFutureFunction) {
        this.rpcGatewayFutureFunction = rpcGatewayFutureFunction;
    }

    // ------------------------------------------------------------------------
    // simple wrappers
    // ------------------------------------------------------------------------

    @Override
    public String getAddress() {
        return backingRpcService.getAddress();
    }

    @Override
    public int getPort() {
        return backingRpcService.getPort();
    }

    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        return backingRpcService.startServer(rpcEndpoint);
    }

    @Override
    public <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken) {
        return backingRpcService.fenceRpcServer(rpcServer, fencingToken);
    }

    @Override
    public void stopServer(RpcServer selfGateway) {
        backingRpcService.stopServer(selfGateway);
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return backingRpcService.getTerminationFuture();
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return backingRpcService.getScheduledExecutor();
    }

    @Override
    public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
        return backingRpcService.scheduleRunnable(runnable, delay, unit);
    }

    @Override
    public void execute(Runnable runnable) {
        backingRpcService.execute(runnable);
    }

    @Override
    public <T> CompletableFuture<T> execute(Callable<T> callable) {
        return backingRpcService.execute(callable);
    }
}
