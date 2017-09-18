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

package org.apache.flink.runtime.webmonitor.retriever;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.util.ExceptionUtils;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Retrieves and stores the leading {@link RpcGateway}.
 *
 * @param <T> type of the gateway to retrieve
 */
public abstract class LeaderGatewayRetriever<T extends RpcGateway> extends LeaderRetriever implements GatewayRetriever<T> {

	private final AtomicReference<CompletableFuture<T>> atomicGatewayFuture;

	private volatile CompletableFuture<T> initialGatewayFuture;

	public LeaderGatewayRetriever() {
		initialGatewayFuture = new CompletableFuture<>();
		atomicGatewayFuture = new AtomicReference<>(initialGatewayFuture);
	}

	@Override
	public CompletableFuture<T> getFuture() {
		final CompletableFuture<T> currentGatewayFuture = atomicGatewayFuture.get();

		if (currentGatewayFuture.isCompletedExceptionally()) {
			try {
				currentGatewayFuture.get();
			} catch (ExecutionException | InterruptedException executionException) {
				String leaderAddress;

				try {
					Tuple2<String, UUID> leaderAddressSessionId = getLeaderNow()
						.orElse(Tuple2.of("unknown address", HighAvailabilityServices.DEFAULT_LEADER_ID));

					leaderAddress = leaderAddressSessionId.f0;
				} catch (Exception e) {
					log.warn("Could not obtain the current leader.", e);
					leaderAddress = "unknown leader address";
				}

				if (log.isDebugEnabled() || log.isTraceEnabled()) {
					// only log exceptions on debug or trace level
					log.warn(
						"Error while retrieving the leader gateway. Retrying to connect to {}.",
						leaderAddress,
						ExceptionUtils.stripExecutionException(executionException));
				} else {
					log.warn(
						"Error while retrieving the leader gateway. Retrying to connect to {}.",
						leaderAddress);
				}
			}

			// we couldn't resolve the gateway --> let's try again
			final CompletableFuture<T> newGatewayFuture = createGateway(getLeaderFuture());

			// let's check if there was a concurrent createNewFuture call
			if (atomicGatewayFuture.compareAndSet(currentGatewayFuture, newGatewayFuture)) {
				return newGatewayFuture;
			} else {
				return atomicGatewayFuture.get();
			}
		} else {
			return atomicGatewayFuture.get();
		}
	}

	@Override
	public void notifyNewLeaderAddress(CompletableFuture<Tuple2<String, UUID>> newLeaderAddressFuture) {
		final CompletableFuture<T> newGatewayFuture = createGateway(newLeaderAddressFuture);

		final CompletableFuture<T> oldGatewayFuture = atomicGatewayFuture.getAndSet(newGatewayFuture);

		// check if the old gateway future was the initial future
		if (oldGatewayFuture == initialGatewayFuture) {
			// we have to complete it because a caller might wait on the initial future
			newGatewayFuture.whenComplete(
				(t, throwable) -> {
					if (throwable != null) {
						oldGatewayFuture.completeExceptionally(throwable);
					} else {
						oldGatewayFuture.complete(t);
					}
				});

			// free the initial gateway future
			initialGatewayFuture = null;
		} else {
			// try to cancel old gateway retrieval operation
			oldGatewayFuture.cancel(false);
		}
	}

	protected abstract CompletableFuture<T> createGateway(CompletableFuture<Tuple2<String, UUID>> leaderFuture);
}
