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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

    public static String waitUntilNewLeaderAppears(long timeout) throws Exception {
        final AtomicReference<String> leaderRef = new AtomicReference<>();
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final String lockIdentity = sharedQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    leaderRef.set(lockIdentity);
                    return lockIdentity != null;
                },
                Deadline.fromNow(Duration.ofMillis(timeout)),
                "No leader is elected with " + timeout + "ms");
        return leaderRef.get();
    }

    public void waitForNewLeader(long timeout) throws Exception {
        final String errorMsg =
                "No leader with " + lockIdentity + " is elected within " + timeout + "ms";
        poll(leaderQueue, timeout, errorMsg);
    }

    public void waitForRevokeLeader(long timeout) throws Exception {
        final String errorMsg =
                "No leader with " + lockIdentity + " is revoke within " + timeout + "ms";
        poll(revokeQueue, timeout, errorMsg);
    }

    private void poll(BlockingQueue<String> queue, long timeout, String errorMsg) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final String lockIdentity = queue.poll(timeout, TimeUnit.MILLISECONDS);
                    return this.lockIdentity.equals(lockIdentity);
                },
                Deadline.fromNow(Duration.ofMillis(timeout)),
                errorMsg);
    }
}
