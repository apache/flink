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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.function.Consumer;

/**
 * {@link LeaderElectionEventHandler} implementation which provides some convenience functions for
 * testing purposes.
 */
public class TestingLeaderElectionEventHandler extends TestingLeaderBase
        implements LeaderElectionEventHandler {

    private final LeaderInformation leaderInformation;

    private final OneShotLatch initializationLatch;

    @Nullable private LeaderElectionDriver initializedLeaderElectionDriver = null;

    private LeaderInformation confirmedLeaderInformation = LeaderInformation.empty();

    public TestingLeaderElectionEventHandler(LeaderInformation leaderInformation) {
        this.leaderInformation = leaderInformation;
        this.initializationLatch = new OneShotLatch();
    }

    public void init(LeaderElectionDriver leaderElectionDriver) {
        Preconditions.checkState(initializedLeaderElectionDriver == null);
        this.initializedLeaderElectionDriver = leaderElectionDriver;
        initializationLatch.trigger();
    }

    @Override
    public void onGrantLeadership() {
        waitForInitialization(
                leaderElectionDriver -> {
                    confirmedLeaderInformation = leaderInformation;
                    leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
                    leaderEventQueue.offer(confirmedLeaderInformation);
                });
    }

    @Override
    public void onRevokeLeadership() {
        waitForInitialization(
                (leaderElectionDriver) -> {
                    confirmedLeaderInformation = LeaderInformation.empty();
                    leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
                    leaderEventQueue.offer(confirmedLeaderInformation);
                });
    }

    @Override
    public void onLeaderInformationChange(LeaderInformation leaderInformation) {
        waitForInitialization(
                leaderElectionDriver -> {
                    if (confirmedLeaderInformation.getLeaderSessionID() != null
                            && !this.confirmedLeaderInformation.equals(leaderInformation)) {
                        leaderElectionDriver.writeLeaderInformation(confirmedLeaderInformation);
                    }
                });
    }

    private void waitForInitialization(Consumer<? super LeaderElectionDriver> operation) {
        try {
            initializationLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Preconditions.checkState(initializedLeaderElectionDriver != null);
        operation.accept(initializedLeaderElectionDriver);
    }

    public LeaderInformation getConfirmedLeaderInformation() {
        return confirmedLeaderInformation;
    }
}
