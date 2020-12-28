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

package org.apache.flink.runtime.rest.handler;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;

/**
 * Tracks in-flight client requests.
 *
 * @see AbstractHandler
 */
@ThreadSafe
class InFlightRequestTracker {

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final Phaser phaser =
            new Phaser(1) {
                @Override
                protected boolean onAdvance(final int phase, final int registeredParties) {
                    terminationFuture.complete(null);
                    return true;
                }
            };

    /**
     * Registers an in-flight request.
     *
     * @return {@code true} if the request could be registered; {@code false} if the tracker has
     *     already been terminated.
     */
    public boolean registerRequest() {
        return phaser.register() >= 0;
    }

    /** Deregisters an in-flight request. */
    public void deregisterRequest() {
        phaser.arriveAndDeregister();
    }

    /**
     * Returns a future that completes when the in-flight requests that were registered prior to
     * calling this method are deregistered.
     */
    public CompletableFuture<Void> awaitAsync() {
        phaser.arriveAndDeregister();
        return terminationFuture;
    }
}
