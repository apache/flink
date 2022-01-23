/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.util.concurrent.CompletableFuture;

/** {@link LeaderRetrievalService} that captures thes {@link LeaderRetrievalListener}. */
final class StartStopNotifyingLeaderRetrievalService implements LeaderRetrievalService {
    private final CompletableFuture<LeaderRetrievalListener> startFuture;

    private final CompletableFuture<Void> stopFuture;

    StartStopNotifyingLeaderRetrievalService(
            CompletableFuture<LeaderRetrievalListener> startFuture,
            CompletableFuture<Void> stopFuture) {
        this.startFuture = startFuture;
        this.stopFuture = stopFuture;
    }

    @Override
    public void start(LeaderRetrievalListener listener) {
        startFuture.complete(listener);
    }

    @Override
    public void stop() {
        stopFuture.complete(null);
    }
}
