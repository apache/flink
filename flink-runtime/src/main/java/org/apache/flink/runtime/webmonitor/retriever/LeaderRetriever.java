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
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/** Retrieves and stores the current leader address. */
public class LeaderRetriever implements LeaderRetrievalListener {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    private AtomicReference<CompletableFuture<Tuple2<String, UUID>>> atomicLeaderFuture;

    public LeaderRetriever() {
        atomicLeaderFuture = new AtomicReference<>(new CompletableFuture<>());
    }

    /**
     * Returns the current leader information if available. Otherwise it returns an empty optional.
     *
     * @return The current leader information if available. Otherwise it returns an empty optional.
     * @throws Exception if the leader future has been completed with an exception
     */
    public Optional<Tuple2<String, UUID>> getLeaderNow() throws Exception {
        CompletableFuture<Tuple2<String, UUID>> leaderFuture = this.atomicLeaderFuture.get();
        if (leaderFuture != null) {
            if (leaderFuture.isDone()) {
                return Optional.of(leaderFuture.get());
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    /** Returns the current JobManagerGateway future. */
    public CompletableFuture<Tuple2<String, UUID>> getLeaderFuture() {
        return atomicLeaderFuture.get();
    }

    @Override
    public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
        final CompletableFuture<Tuple2<String, UUID>> newLeaderFuture;

        if (isEmptyAddress(leaderAddress)) {
            newLeaderFuture = new CompletableFuture<>();
        } else {
            newLeaderFuture =
                    CompletableFuture.completedFuture(Tuple2.of(leaderAddress, leaderSessionID));
        }

        try {
            final CompletableFuture<Tuple2<String, UUID>> oldLeaderFuture =
                    atomicLeaderFuture.getAndSet(newLeaderFuture);

            if (!oldLeaderFuture.isDone()) {
                newLeaderFuture.whenComplete(
                        (stringUUIDTuple2, throwable) -> {
                            if (throwable != null) {
                                oldLeaderFuture.completeExceptionally(throwable);
                            } else {
                                oldLeaderFuture.complete(stringUUIDTuple2);
                            }
                        });
            }

            notifyNewLeaderAddress(newLeaderFuture);
        } catch (Exception e) {
            handleError(e);
        }
    }

    private boolean isEmptyAddress(String leaderAddress) {
        return leaderAddress == null || leaderAddress.equals("");
    }

    @Override
    public void handleError(Exception exception) {
        log.error("Received error from LeaderRetrievalService.", exception);

        atomicLeaderFuture.get().completeExceptionally(exception);
    }

    protected void notifyNewLeaderAddress(
            CompletableFuture<Tuple2<String, UUID>> newLeaderAddressFuture) {}
}
