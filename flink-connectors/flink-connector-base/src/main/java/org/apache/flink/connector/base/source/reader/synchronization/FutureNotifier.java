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

package org.apache.flink.connector.base.source.reader.synchronization;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A class facilitating the asynchronous communication among threads.
 */
public class FutureNotifier {
	/** A future reference. */
	private final AtomicReference<CompletableFuture<Void>> futureRef;

	public FutureNotifier() {
		this.futureRef = new AtomicReference<>(null);
	}

	/**
	 * Get the future out of this notifier. The future will be completed when someone invokes
	 * {@link #notifyComplete()}. If there is already an uncompleted future, that existing
	 * future will be returned instead of a new one.
	 *
	 * @return a future that will be completed when {@link #notifyComplete()} is invoked.
	 */
	public CompletableFuture<Void> future() {
		CompletableFuture<Void> prevFuture = futureRef.get();
		if (prevFuture != null) {
			// Someone has created a future for us, don't create a new one.
			return prevFuture;
		} else {
			CompletableFuture<Void> newFuture = new CompletableFuture<>();
			boolean newFutureSet = futureRef.compareAndSet(null, newFuture);
			// If someone created a future after our previous check, use that future.
			// Otherwise, use the new future.
			return newFutureSet ? newFuture : future();
		}
	}

	/**
	 * Complete the future if there is one. This will release the thread that is waiting for data.
	 */
	public void notifyComplete() {
		CompletableFuture<Void> future = futureRef.get();
		// If there are multiple threads trying to complete the future, only the first one succeeds.
		if (future != null && future.complete(null)) {
			futureRef.compareAndSet(future, null);
		}
	}
}
