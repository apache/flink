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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility base class for testing gateways
 */
public abstract class TestingGatewayBase implements RpcGateway {

	private final ScheduledExecutorService executor;

	private final String address;

	protected TestingGatewayBase(final String address) {
		this.executor = Executors.newSingleThreadScheduledExecutor();
		this.address = address;
	}

	protected TestingGatewayBase() {
		this("localhost");
	}

	// ------------------------------------------------------------------------
	//  shutdown
	// ------------------------------------------------------------------------

	public void stop() {
		executor.shutdownNow();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		executor.shutdownNow();
	}

	// ------------------------------------------------------------------------
	//  Base class methods
	// ------------------------------------------------------------------------

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public String getHostname() {
		return address;
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	public <T> CompletableFuture<T> futureWithTimeout(long timeoutMillis) {
		CompletableFuture<T> future = new CompletableFuture<>();
		executor.schedule(new FutureTimeout(future), timeoutMillis, TimeUnit.MILLISECONDS);
		return future;
	}

	// ------------------------------------------------------------------------
	
	private static final class FutureTimeout implements Runnable {

		private final CompletableFuture<?> promise;

		private FutureTimeout(CompletableFuture<?> promise) {
			this.promise = promise;
		}

		@Override
		public void run() {
			try {
				promise.completeExceptionally(new TimeoutException());
			} catch (Throwable t) {
				System.err.println("CAUGHT AN ERROR IN THE TEST: " + t.getMessage());
				t.printStackTrace();
			}
		}
	}
}
