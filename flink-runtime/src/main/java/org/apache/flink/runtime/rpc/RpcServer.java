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
import org.apache.flink.runtime.rpc.akka.RunnableAkkaGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.concurrent.Callable;

/**
 * Base class for rpc servers. Every rpc server should implement this interface.
 *
 * @param <C> Rpc gateway counter part matching the RpcServer
 */
public abstract class RpcServer<C extends RpcGateway> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final RpcService rpcService;
	private C self;
	private MainThreadExecutionContext mainThreadExecutionContext;

	public RpcServer(RpcService rpcService) {
		this.rpcService = rpcService;
	}

	/**
	 * Get self-gateway which should be used to run asynchronous rpc calls on the server.
	 *
	 * IMPORTANT: Always issue local method calls via the self-gateway if the current thread
	 * is not the main thread of the rpc server, e.g. from within a future callback.
	 *
	 * @return Self gateway
	 */
	public C getSelf() {
		return self;
	}

	public void runAsync(Runnable runnable) {
		((RunnableAkkaGateway) self).runAsync(runnable);
	}

	public <V> Future<V> callAsync(Callable<V> callable, Timeout timeout) {
		return ((RunnableAkkaGateway) self).callAsync(callable, timeout);
	}

	public ExecutionContext getMainThreadExecutionContext() {
		return mainThreadExecutionContext;
	}

	public RpcService getRpcService() {
		return rpcService;
	}

	public void start() {
		self = rpcService.startServer(this);
		mainThreadExecutionContext = new MainThreadExecutionContext((RunnableAkkaGateway) self);
	}

	public void shutDown() {
		rpcService.stopServer(self);
	}

	public String getAddress() {
		return rpcService.getAddress(self);
	}

	public class MainThreadExecutionContext implements ExecutionContext {
		private final RunnableAkkaGateway gateway;

		public MainThreadExecutionContext(RunnableAkkaGateway gateway) {
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
