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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.concurrent.Callable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for rpc protocols. Distributed components which offer remote procedure calls have to
 * extend the rpc protocol base class.
 *
 * <p>The main idea is that a rpc protocol is backed by a rpc server which has a single thread
 * processing the rpc calls. Thus, by executing all state changing operations within the main
 * thread, we don't have to reason about concurrent accesses. The rpc provides provides
 * {@link #runAsync(Runnable)}, {@link #callAsync(Callable, Timeout)} and the
 * {@link #getMainThreadExecutionContext()} to execute code in the rpc server's main thread.
 *
 * @param <C> Rpc gateway counterpart for the implementing rpc protocol
 */
public abstract class RpcProtocol<C extends RpcGateway> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	// ------------------------------------------------------------------------

	/** RPC service to be used to start the rpc server and to obtain rpc gateways */
	private final RpcService rpcService;

	/** Self gateway which can be used to schedule asynchronous calls on yourself */
	private final C self;

	/** the fully qualified address of the this RPC endpoint */
	private final String selfAddress;

	/** The main thread execution context to be used to execute future callbacks in the main thread
	 * of the executing rpc server. */
	private final MainThreadExecutionContext mainThreadExecutionContext;


	public RpcProtocol(RpcService rpcService) {
		this.rpcService = checkNotNull(rpcService, "rpcService");
		this.self = rpcService.startServer(this);
		this.selfAddress = rpcService.getAddress(self);
		this.mainThreadExecutionContext = new MainThreadExecutionContext((MainThreadExecutor) self);
	}

	// ------------------------------------------------------------------------
	//  Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Shuts down the underlying rpc server via the rpc service.
	 *
	 * Can be overridden to add rpc protocol specific shut down code. Should always call the parent
	 * shut down method.
	 */
	public void shutDown() {
		rpcService.stopServer(self);
	}

	// ------------------------------------------------------------------------
	//  Basic RPC endpoint properties
	// ------------------------------------------------------------------------

	/**
	 * Get self-gateway which should be used to run asynchronous rpc calls on this protocol.
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
	 * Gets the address of the underlying rpc server. The address should be fully qualified so that
	 * a remote system can connect to this rpc server via this address.
	 *
	 * @return Fully qualified address of the underlying rpc server
	 */
	public String getAddress() {
		return selfAddress;
	}

	/**
	 * Gets the main thread execution context. The main thread execution context can be used to
	 * execute tasks in the main thread of the underlying rpc server.
	 *
	 * @return Main thread execution context
	 */
	public ExecutionContext getMainThreadExecutionContext() {
		return mainThreadExecutionContext;
	}

	/**
	 * Gets the used rpc service.
	 *
	 * @return Rpc service
	 */
	public RpcService getRpcService() {
		return rpcService;
	}

	// ------------------------------------------------------------------------
	//  Asynchronous executions
	// ------------------------------------------------------------------------

	/**
	 * Execute the runnable in the main thread of the underlying rpc server.
	 *
	 * @param runnable Runnable to be executed in the main thread of the underlying rpc server
	 */
	public void runAsync(Runnable runnable) {
		((MainThreadExecutor) self).runAsync(runnable);
	}

	/**
	 * Execute the callable in the main thread of the underlying rpc server returning a future for
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
