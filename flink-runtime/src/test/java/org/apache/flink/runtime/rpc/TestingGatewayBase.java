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

import akka.dispatch.Futures;
import scala.concurrent.Future;
import scala.concurrent.Promise;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility base class for testing gateways
 */
public abstract class TestingGatewayBase implements RpcGateway {

	private final ScheduledExecutorService executor;

	protected TestingGatewayBase() {
		this.executor = Executors.newSingleThreadScheduledExecutor();
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
	//  utilities
	// ------------------------------------------------------------------------

	public <T> Future<T> futureWithTimeout(long timeoutMillis) {
		Promise<T> promise = Futures.<T>promise();
		executor.schedule(new FutureTimeout(promise), timeoutMillis, TimeUnit.MILLISECONDS);
		return promise.future();
	}

	// ------------------------------------------------------------------------
	
	private static final class FutureTimeout implements Runnable {

		private final Promise<?> promise;

		private FutureTimeout(Promise<?> promise) {
			this.promise = promise;
		}

		@Override
		public void run() {
			try {
				promise.failure(new TimeoutException());
			} catch (Throwable t) {
				System.err.println("CAUGHT AN ERROR IN THE TEST: " + t.getMessage());
				t.printStackTrace();
			}
		}
	}
}
