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

import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Interface for rpc services. An rpc service is used to start and connect to a {@link RpcEndpoint}.
 * Connecting to a rpc server will return a {@link RpcGateway} which can be used to call remote
 * procedures.
 */
public interface RpcService {

    /**
     * Return the hostname or host address under which the rpc service can be reached. If the rpc
     * service cannot be contacted remotely, then it will return an empty string.
     *
     * @return Address of the rpc service or empty string if local rpc service
     */
    String getAddress();

    /**
     * Return the port under which the rpc service is reachable. If the rpc service cannot be
     * contacted remotely, then it will return -1.
     *
     * @return Port of the rpc service or -1 if local rpc service
     */
    int getPort();

    /**
     * Connect to a remote rpc server under the provided address. Returns a rpc gateway which can be
     * used to communicate with the rpc server. If the connection failed, then the returned future
     * is failed with a {@link RpcConnectionException}.
     *
     * @param address Address of the remote rpc server
     * @param clazz Class of the rpc gateway to return
     * @param <C> Type of the rpc gateway to return
     * @return Future containing the rpc gateway or an {@link RpcConnectionException} if the
     *     connection attempt failed
     */
    <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz);

    /**
     * Connect to a remote fenced rpc server under the provided address. Returns a fenced rpc
     * gateway which can be used to communicate with the rpc server. If the connection failed, then
     * the returned future is failed with a {@link RpcConnectionException}.
     *
     * @param address Address of the remote rpc server
     * @param fencingToken Fencing token to be used when communicating with the server
     * @param clazz Class of the rpc gateway to return
     * @param <F> Type of the fencing token
     * @param <C> Type of the rpc gateway to return
     * @return Future containing the fenced rpc gateway or an {@link RpcConnectionException} if the
     *     connection attempt failed
     */
    <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz);

    /**
     * Start a rpc server which forwards the remote procedure calls to the provided rpc endpoint.
     *
     * @param rpcEndpoint Rpc protocol to dispatch the rpcs to
     * @param <C> Type of the rpc endpoint
     * @return Self gateway to dispatch remote procedure calls to oneself
     */
    <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint);

    /**
     * Fence the given RpcServer with the given fencing token.
     *
     * <p>Fencing the RpcServer means that we fix the fencing token to the provided value. All RPCs
     * will then be enriched with this fencing token. This expects that the receiving RPC endpoint
     * extends {@link FencedRpcEndpoint}.
     *
     * @param rpcServer to fence with the given fencing token
     * @param fencingToken to fence the RpcServer with
     * @param <F> type of the fencing token
     * @return Fenced RpcServer
     */
    <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken);

    /**
     * Stop the underlying rpc server of the provided self gateway.
     *
     * @param selfGateway Self gateway describing the underlying rpc server
     */
    void stopServer(RpcServer selfGateway);

    /**
     * Trigger the asynchronous stopping of the {@link RpcService}.
     *
     * @return Future which is completed once the {@link RpcService} has been fully stopped.
     */
    CompletableFuture<Void> stopService();

    /**
     * Returns a future indicating when the RPC service has been shut down.
     *
     * @return Termination future
     */
    CompletableFuture<Void> getTerminationFuture();

    /**
     * Gets a scheduled executor from the RPC service. This executor can be used to schedule tasks
     * to be executed in the future.
     *
     * <p><b>IMPORTANT:</b> This executor does not isolate the method invocations against any
     * concurrent invocations and is therefore not suitable to run completion methods of futures
     * that modify state of an {@link RpcEndpoint}. For such operations, one needs to use the {@link
     * RpcEndpoint#getMainThreadExecutor() MainThreadExecutionContext} of that {@code RpcEndpoint}.
     *
     * @return The RPC service provided scheduled executor
     */
    ScheduledExecutor getScheduledExecutor();

    /**
     * Execute the runnable in the execution context of this RPC Service, as returned by {@link
     * #getScheduledExecutor()} ()}, after a scheduled delay.
     *
     * @param runnable Runnable to be executed
     * @param delay The delay after which the runnable will be executed
     */
    ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit);

    /**
     * Execute the given runnable in the executor of the RPC service. This method can be used to run
     * code outside of the main thread of a {@link RpcEndpoint}.
     *
     * <p><b>IMPORTANT:</b> This executor does not isolate the method invocations against any
     * concurrent invocations and is therefore not suitable to run completion methods of futures
     * that modify state of an {@link RpcEndpoint}. For such operations, one needs to use the {@link
     * RpcEndpoint#getMainThreadExecutor() MainThreadExecutionContext} of that {@code RpcEndpoint}.
     *
     * @param runnable to execute
     */
    void execute(Runnable runnable);

    /**
     * Execute the given callable and return its result as a {@link CompletableFuture}. This method
     * can be used to run code outside of the main thread of a {@link RpcEndpoint}.
     *
     * <p><b>IMPORTANT:</b> This executor does not isolate the method invocations against any
     * concurrent invocations and is therefore not suitable to run completion methods of futures
     * that modify state of an {@link RpcEndpoint}. For such operations, one needs to use the {@link
     * RpcEndpoint#getMainThreadExecutor() MainThreadExecutionContext} of that {@code RpcEndpoint}.
     *
     * @param callable to execute
     * @param <T> is the return value type
     * @return Future containing the callable's future result
     */
    <T> CompletableFuture<T> execute(Callable<T> callable);
}
