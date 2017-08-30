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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for RPC endpoints. Distributed components which offer remote procedure calls have to
 * extend the RPC endpoint base class. An RPC endpoint is backed by an {@link RpcService}. 
 * 
 * <h1>Endpoint and Gateway</h1>
 * 
 * To be done...
 * 
 * <h1>Single Threaded Endpoint Execution </h1>
 * 
 * <p>All RPC calls on the same endpoint are called by the same thread
 * (referred to as the endpoint's <i>main thread</i>).
 * Thus, by executing all state changing operations within the main 
 * thread, we don't have to reason about concurrent accesses, in the same way in the Actor Model
 * of Erlang or Akka.
 *
 * <p>The RPC endpoint provides provides {@link #runAsync(Runnable)}, {@link #callAsync(Callable, Time)}
 * and the {@link #getMainThreadExecutor()} to execute code in the RPC endpoint's main thread.
 */
public abstract class RpcEndpoint implements RpcGateway {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	// ------------------------------------------------------------------------

	/** RPC service to be used to start the RPC server and to obtain rpc gateways */
	private final RpcService rpcService;

	/** Unique identifier for this rpc endpoint */
	private final String endpointId;

	/** Interface to access the underlying rpc server */
	private final RpcServer rpcServer;

	/** The main thread executor to be used to execute future callbacks in the main thread
	 * of the executing rpc server. */
	private final Executor mainThreadExecutor;

	/** A reference to the endpoint's main thread, if the current method is called by the main thread */
	final AtomicReference<Thread> currentMainThread = new AtomicReference<>(null); 

	/**
	 * Initializes the RPC endpoint.
	 * 
	 * @param rpcService The RPC server that dispatches calls to this RPC endpoint.
	 * @param endpointId Unique identifier for this endpoint
	 */
	protected RpcEndpoint(final RpcService rpcService, final String endpointId) {
		this.rpcService = checkNotNull(rpcService, "rpcService");
		this.endpointId = checkNotNull(endpointId, "endpointId");

		this.rpcServer = rpcService.startServer(this);

		this.mainThreadExecutor = new MainThreadExecutor(rpcServer);
	}

	/**
	 * Initializes the RPC endpoint with a random endpoint id.
	 *
	 * @param rpcService The RPC server that dispatches calls to this RPC endpoint.
	 */
	protected RpcEndpoint(final RpcService rpcService) {
		this(rpcService, UUID.randomUUID().toString());
	}

	/**
	 * Returns the rpc endpoint's identifier.
	 *
	 * @return Rpc endpoint's identifier.
	 */
	public String getEndpointId() {
		return endpointId;
	}

	// ------------------------------------------------------------------------
	//  Start & shutdown & lifecycle callbacks
	// ------------------------------------------------------------------------

	/**
	 * Starts the rpc endpoint. This tells the underlying rpc server that the rpc endpoint is ready
	 * to process remote procedure calls.
	 *
	 * IMPORTANT: Whenever you override this method, call the parent implementation to enable
	 * rpc processing. It is advised to make the parent call last.
	 *
	 * @throws Exception indicating that something went wrong while starting the RPC endpoint
	 */
	public void start() throws Exception {
		rpcServer.start();
	}

	/**
	 * Stops the rpc endpoint. This tells the underlying rpc server that the rpc endpoint is
	 * no longer ready to process remote procedure calls.
	 */
	protected final void stop() {
		rpcServer.stop();
	}

	/**
	 * User overridable callback.
	 *
	 * <p>This method is called when the RpcEndpoint is being shut down. The method is guaranteed
	 * to be executed in the main thread context and can be used to clean up internal state.
	 *
	 * IMPORTANT: This method should never be called directly by the user.
	 *
	 * @throws Exception if an error occurs. The exception is returned as result of the termination future.
	 */
	public void postStop() throws Exception {}

	/**
	 * Triggers the shut down of the rpc endpoint. The shut down is executed asynchronously.
	 *
	 * <p>In order to wait on the completion of the shut down, obtain the termination future
	 * via {@link #getTerminationFuture()}} and wait on its completion.
	 */
	public final void shutDown() {
		rpcService.stopServer(rpcServer);
	}

	// ------------------------------------------------------------------------
	//  Basic RPC endpoint properties
	// ------------------------------------------------------------------------

	/**
	 * Returns a self gateway of the specified type which can be used to issue asynchronous
	 * calls against the RpcEndpoint.
	 *
	 * <p>IMPORTANT: The self gateway type must be implemented by the RpcEndpoint. Otherwise
	 * the method will fail.
	 *
	 * @param selfGatewayType class of the self gateway type
	 * @param <C> type of the self gateway to create
	 * @return Self gateway of the specified type which can be used to issue asynchronous rpcs
	 */
	public <C extends RpcGateway> C getSelfGateway(Class<C> selfGatewayType) {
		if (selfGatewayType.isInstance(rpcServer)) {
			@SuppressWarnings("unchecked")
			C selfGateway = ((C) rpcServer);

			return selfGateway;
		} else {
			throw new RuntimeException("RpcEndpoint does not implement the RpcGateway interface of type " + selfGatewayType + '.');
		}
	}

	/**
	 * Gets the address of the underlying RPC endpoint. The address should be fully qualified so that
	 * a remote system can connect to this RPC endpoint via this address.
	 *
	 * @return Fully qualified address of the underlying RPC endpoint
	 */
	@Override
	public String getAddress() {
		return rpcServer.getAddress();
	}

	/**
	 * Gets the hostname of the underlying RPC endpoint.
	 *
	 * @return Hostname on which the RPC endpoint is running
	 */
	@Override
	public String getHostname() {
		return rpcServer.getHostname();
	}

	/**
	 * Gets the main thread execution context. The main thread execution context can be used to
	 * execute tasks in the main thread of the underlying RPC endpoint.
	 *
	 * @return Main thread execution context
	 */
	protected Executor getMainThreadExecutor() {
		return mainThreadExecutor;
	}

	/**
	 * Gets the endpoint's RPC service.
	 *
	 * @return The endpoint's RPC service
	 */
	public RpcService getRpcService() {
		return rpcService;
	}

	/**
	 * Return a future which is completed when the rpc endpoint has been terminated.
	 *
	 * @return Future which is completed when the rpc endpoint has been terminated.
	 */
	public CompletableFuture<Void> getTerminationFuture() {
		return rpcServer.getTerminationFuture();
	}

	// ------------------------------------------------------------------------
	//  Asynchronous executions
	// ------------------------------------------------------------------------


	/**
	 * Execute the runnable in the main thread of the underlying RPC endpoint.
	 *
	 * @param runnable Runnable to be executed in the main thread of the underlying RPC endpoint
	 */
	protected void runAsync(Runnable runnable) {
		rpcServer.runAsync(runnable);
	}

	/**
	 * Execute the runnable in the main thread of the underlying RPC endpoint, with
	 * a delay of the given number of milliseconds.
	 *
	 * @param runnable Runnable to be executed
	 * @param delay    The delay after which the runnable will be executed
	 */
	protected void scheduleRunAsync(Runnable runnable, Time delay) {
		scheduleRunAsync(runnable, delay.getSize(), delay.getUnit());
	}

	/**
	 * Execute the runnable in the main thread of the underlying RPC endpoint, with
	 * a delay of the given number of milliseconds.
	 *
	 * @param runnable Runnable to be executed
	 * @param delay    The delay after which the runnable will be executed
	 */
	protected void scheduleRunAsync(Runnable runnable, long delay, TimeUnit unit) {
		rpcServer.scheduleRunAsync(runnable, unit.toMillis(delay));
	}

	/**
	 * Execute the callable in the main thread of the underlying RPC service, returning a future for
	 * the result of the callable. If the callable is not completed within the given timeout, then
	 * the future will be failed with a {@link TimeoutException}.
	 *
	 * @param callable Callable to be executed in the main thread of the underlying rpc server
	 * @param timeout Timeout for the callable to be completed
	 * @param <V> Return type of the callable
	 * @return Future for the result of the callable.
	 */
	protected <V> CompletableFuture<V> callAsync(Callable<V> callable, Time timeout) {
		return rpcServer.callAsync(callable, timeout);
	}

	// ------------------------------------------------------------------------
	//  Main Thread Validation
	// ------------------------------------------------------------------------

	/**
	 * Validates that the method call happens in the RPC endpoint's main thread.
	 * 
	 * <p><b>IMPORTANT:</b> This check only happens when assertions are enabled,
	 * such as when running tests.
	 * 
	 * <p>This can be used for additional checks, like
	 * <pre>{@code
	 * protected void concurrencyCriticalMethod() {
	 *     validateRunsInMainThread();
	 *     
	 *     // some critical stuff
	 * }
	 * }</pre>
	 */
	public void validateRunsInMainThread() {
		assert currentMainThread.get() == Thread.currentThread();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	/**
	 * Executor which executes runnables in the main thread context.
	 */
	private static class MainThreadExecutor implements Executor {

		private final MainThreadExecutable gateway;

		MainThreadExecutor(MainThreadExecutable gateway) {
			this.gateway = Preconditions.checkNotNull(gateway);
		}

		@Override
		public void execute(@Nonnull Runnable runnable) {
			gateway.runAsync(runnable);
		}
	}
}
