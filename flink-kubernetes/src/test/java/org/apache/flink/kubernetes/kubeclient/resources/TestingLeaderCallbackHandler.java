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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.util.Preconditions;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;

/** Testing implementation for {@link KubernetesLeaderElector.LeaderCallbackHandler}. */
public class TestingLeaderCallbackHandler extends KubernetesLeaderElector.LeaderCallbackHandler {

    private static final BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<>();

    private final BlockingQueue<String> leaderQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> revokeQueue = new LinkedBlockingQueue<>();
    private final String lockIdentity;

    private boolean isLeader;

    public TestingLeaderCallbackHandler(String lockIdentity) {
        this.lockIdentity = lockIdentity;
    }

    @Override
    public void isLeader() {
        isLeader = true;
        leaderQueue.offer(lockIdentity);
        sharedQueue.offer(lockIdentity);
    }

    @Override
    public void notLeader() {
        isLeader = false;
        revokeQueue.offer(lockIdentity);
    }

    public String getLockIdentity() {
        return lockIdentity;
    }

    public boolean hasLeadership() {
        return isLeader;
    }

    public static String waitUntilNewLeaderAppears() throws Exception {
        return retrieveNextEventAsync(sharedQueue).get();
    }

    public static CompletableFuture<String> retrieveNextEventAsync(
            BlockingQueue<String> eventQueue) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return eventQueue.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                });
    }

    public void waitForNewLeader() throws Exception {
        waitForNewLeaderAsync().get();
    }

    public CompletableFuture<Void> waitForNewLeaderAsync() {
        return waitForNextEvent(leaderQueue);
    }

    public void waitForRevokeLeader() throws Exception {
        waitForRevokeLeaderAsync().get();
    }

    public CompletableFuture<Void> waitForRevokeLeaderAsync() {
        return waitForNextEvent(revokeQueue);
    }

    private CompletableFuture<Void> waitForNextEvent(BlockingQueue<String> eventQueue) {
        return retrieveNextEventAsync(eventQueue)
                .thenAccept(
                        eventLockIdentity ->
                                Preconditions.checkState(eventLockIdentity.equals(lockIdentity)));
    }
}
