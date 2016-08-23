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
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.concurrent.TimeUnit;

/**
 * Interface for rpc services. An rpc service is used to start and connect to a {@link RpcEndpoint}.
 * Connecting to a rpc server will return a {@link RpcGateway} which can be used to call remote
 * procedures.
 */
public interface RpcService {

	/**
	 * Connect to a remote rpc server under the provided address. Returns a rpc gateway which can
	 * be used to communicate with the rpc server. If the connection failed, then the returned
	 * future is failed with a {@link RpcConnectionException}.
	 *
	 * @param address Address of the remote rpc server
	 * @param clazz Class of the rpc gateway to return
	 * @param <C> Type of the rpc gateway to return
	 * @return Future containing the rpc gateway or an {@link RpcConnectionException} if the
	 * connection attempt failed
	 */
	<C extends RpcGateway> Future<C> connect(String address, Class<C> clazz);

	/**
	 * Start a rpc server which forwards the remote procedure calls to the provided rpc endpoint.
	 *
	 * @param rpcEndpoint Rpc protocl to dispath the rpcs to
	 * @param <S> Type of the rpc endpoint
	 * @param <C> Type of the self rpc gateway associated with the rpc server
	 * @return Self gateway to dispatch remote procedure calls to oneself
	 */
	<C extends RpcGateway, S extends RpcEndpoint<C>> C startServer(S rpcEndpoint);

	/**
	 * Stop the underlying rpc server of the provided self gateway.
	 *
	 * @param selfGateway Self gateway describing the underlying rpc server
	 * @param <C> Type of the rpc gateway
	 */
	<C extends RpcGateway> void stopServer(C selfGateway);

	/**
	 * Stop the rpc service shutting down all started rpc servers.
	 */
	void stopService();

	/**
	 * Gets the execution context, provided by this RPC service. This execution
	 * context can be used for example for the {@code onComplete(...)} or {@code onSuccess(...)}
	 * methods of Futures.
	 * 
	 * <p><b>IMPORTANT:</b> This execution context does not isolate the method invocations against
	 * any concurrent invocations and is therefore not suitable to run completion methods of futures
	 * that modify state of an {@link RpcEndpoint}. For such operations, one needs to use the
	 * {@link RpcEndpoint#getMainThreadExecutionContext() MainThreadExecutionContext} of that
	 * {@code RpcEndpoint}.
	 * 
	 * @return The execution context provided by the RPC service
	 */
	ExecutionContext getExecutionContext();

	/**
	 * Execute the runnable in the execution context of this RPC Service, as returned by
	 * {@link #getExecutionContext()}, after a scheduled delay.
	 *
	 * @param runnable Runnable to be executed
	 * @param delay    The delay after which the runnable will be executed
	 */
	void scheduleRunnable(Runnable runnable, long delay, TimeUnit unit);
}
