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

import akka.util.Timeout;

import org.apache.flink.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
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
 * <p>The RPC endpoint provides provides {@link #runAsync(Runnable)}, {@link #callAsync(Callable, Timeout)}
  * and the {@link #getMainThreadExecutionContext()} to execute code in the RPC endoint's main thread.
 *
 * @param <C> The RPC gateway counterpart for the implementing RPC endpoint
 */
public abstract class RpcEndpoint<C extends RpcGateway> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	// ------------------------------------------------------------------------

	/** RPC service to be used to start the RPC server and to obtain rpc gateways */
	private final RpcService rpcService;

	/** Class of the self gateway */
	private final Class<C> selfGatewayType;

	/** Self gateway which can be used to schedule asynchronous calls on yourself */
	private final C self;

	/** the fully qualified address of the this RPC endpoint */
	private final String selfAddress;

	/** The main thread execution context to be used to execute future callbacks in the main thread
	 * of the executing rpc server. */
	private final ExecutionContext mainThreadExecutionContext;

	/** A reference to the endpoint's main thread, if the current method is called by the main thread */
	final AtomicReference<Thread> currentMainThread = new AtomicReference<>(null); 

	/**
	 * Initializes the RPC endpoint.
	 * 
	 * @param rpcService The RPC server that dispatches calls to this RPC endpoint. 
	 */
	protected RpcEndpoint(final RpcService rpcService) {
		this.rpcService = checkNotNull(rpcService, "rpcService");

		// IMPORTANT: Don't change order of selfGatewayType and self because rpcService.startServer
		// requires that selfGatewayType has been initialized
		this.selfGatewayType = ReflectionUtil.getTemplateType1(getClass());
		this.self = rpcService.startServer(this);
		
		this.selfAddress = rpcService.getAddress(self);
		this.mainThreadExecutionContext = new MainThreadExecutionContext((MainThreadExecutor) self);
	}

	/**
	 * Returns the class of the self gateway type.
	 *
	 * @return Class of the self gateway type
	 */
	public final Class<C> getSelfGatewayType() {
		return selfGatewayType;
	}
	
	// ------------------------------------------------------------------------
	//  Start & Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Starts the rpc endpoint. This tells the underlying rpc server that the rpc endpoint is ready
	 * to process remote procedure calls.
	 *
	 * IMPORTANT: Whenever you override this method, call the parent implementation to enable
	 * rpc processing. It is advised to make the parent call last.
	 */
	public void start() {
		((StartStoppable) self).start();
	}

	/**
	 * Shuts down the underlying RPC endpoint via the RPC service.
	 * After this method was called, the RPC endpoint will no longer be reachable, neither remotely,
	 * not via its {@link #getSelf() self gateway}. It will also not accepts executions in main thread
	 * any more (via {@link #callAsync(Callable, Timeout)} and {@link #runAsync(Runnable)}).
	 * 
	 * <p>This method can be overridden to add RPC endpoint specific shut down code.
	 * The overridden method should always call the parent shut down method.
	 */
	public void shutDown() {
		rpcService.stopServer(self);
	}

	// ------------------------------------------------------------------------
	//  Basic RPC endpoint properties
	// ------------------------------------------------------------------------

	/**
	 * Get self-gateway which should be used to run asynchronous RPC calls on this endpoint.
	 *
	 * <p><b>IMPORTANT</b>: Always issue local method calls via the self-gateway if the current thread
	 * is not the main thread of the underlying rpc server, e.g. from within a future callback.
	 *
	 * @return The self gateway
	 */
	public C getSelf() {
		return self;
	}

	/**
	 * Gets the address of the underlying RPC endpoint. The address should be fully qualified so that
	 * a remote system can connect to this RPC endpoint via this address.
	 *
	 * @return Fully qualified address of the underlying RPC endpoint
	 */
	public String getAddress() {
		return selfAddress;
	}

	/**
	 * Gets the main thread execution context. The main thread execution context can be used to
	 * execute tasks in the main thread of the underlying RPC endpoint.
	 *
	 * @return Main thread execution context
	 */
	public ExecutionContext getMainThreadExecutionContext() {
		return mainThreadExecutionContext;
	}

	/**
	 * Gets the endpoint's RPC service.
	 *
	 * @return The endpoint's RPC service
	 */
	public RpcService getRpcService() {
		return rpcService;
	}

	// ------------------------------------------------------------------------
	//  Asynchronous executions
	// ------------------------------------------------------------------------


	/**
	 * Execute the runnable in the main thread of the underlying RPC endpoint.
	 *
	 * @param runnable Runnable to be executed in the main thread of the underlying RPC endpoint
	 */
	public void runAsync(Runnable runnable) {
		((MainThreadExecutor) self).runAsync(runnable);
	}

	/**
	 * Execute the runnable in the main thread of the underlying RPC endpoint, with
	 * a delay of the given number of milliseconds.
	 *
	 * @param runnable Runnable to be executed
	 * @param delay    The delay after which the runnable will be executed
	 */
	public void scheduleRunAsync(Runnable runnable, long delay, TimeUnit unit) {
		((MainThreadExecutor) self).scheduleRunAsync(runnable, unit.toMillis(delay));
	}

	/**
	 * Execute the callable in the main thread of the underlying RPC service, returning a future for
	 * the result of the callable. If the callable is not completed within the given timeout, then
	 * the future will be failed with a {@link java.util.concurrent.TimeoutException}.
	 *
	 * @param callable Callable to be executed in the main thread of the underlying rpc server
	 * @param timeout Timeout for the callable to be completed
	 * @param <V> Return type of the callable
	 * @return Future for the result of the callable.
	 */
	public <V> Future<V> callAsync(Callable<V> callable, Timeout timeout) {
		return ((MainThreadExecutor) self).callAsync(callable, timeout);
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
	 * Execution context which executes runnables in the main thread context. A reported failure
	 * will cause the underlying rpc server to shut down.
	 */
	private class MainThreadExecutionContext implements ExecutionContext {

		private final MainThreadExecutor gateway;

		MainThreadExecutionContext(MainThreadExecutor gateway) {
			this.gateway = gateway;
		}

		@Override
		public void execute(Runnable runnable) {
			gateway.runAsync(runnable);
		}

		@Override
		public void reportFailure(final Throwable t) {
			gateway.runAsync(new Runnable() {
				@Override
				public void run() {
					log.error("Encountered failure in the main thread execution context.", t);
					shutDown();
				}
			});
		}

		@Override
		public ExecutionContext prepare() {
			return this;
		}
	}
}
