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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Leader service for {@link TestingManualHighAvailabilityServices} implementation. The leader
 * service allows to create multiple {@link TestingLeaderElectionService} and {@link
 * SettableLeaderRetrievalService} and allows to manually trigger the services identified by a
 * continuous index.
 */
public class ManualLeaderService {

    private final List<TestingLeaderElectionService> leaderElectionServices;
    private final List<SettableLeaderRetrievalService> leaderRetrievalServices;

    private int currentLeaderIndex;

    @Nullable private UUID currentLeaderId;

    public ManualLeaderService() {
        leaderElectionServices = new ArrayList<>(4);
        leaderRetrievalServices = new ArrayList<>(4);

        currentLeaderIndex = -1;
        currentLeaderId = null;
    }

    public LeaderRetrievalService createLeaderRetrievalService() {
        final SettableLeaderRetrievalService settableLeaderRetrievalService =
                new SettableLeaderRetrievalService(
                        getLeaderAddress(currentLeaderIndex), currentLeaderId);

        leaderRetrievalServices.add(settableLeaderRetrievalService);

        return settableLeaderRetrievalService;
    }

    public LeaderElectionService createLeaderElectionService() {
        TestingLeaderElectionService testingLeaderElectionService =
                new TestingLeaderElectionService();

        leaderElectionServices.add(testingLeaderElectionService);

        return testingLeaderElectionService;
    }

    public void grantLeadership(int index, UUID leaderId) {
        if (currentLeaderId != null) {
            revokeLeadership();
        }

        Preconditions.checkNotNull(leaderId);
        Preconditions.checkArgument(0 <= index && index < leaderElectionServices.size());

        TestingLeaderElectionService testingLeaderElectionService =
                leaderElectionServices.get(index);

        testingLeaderElectionService.isLeader(leaderId);

        currentLeaderIndex = index;
        currentLeaderId = leaderId;
    }

    public void revokeLeadership() {
        assert (currentLeaderId != null);
        assert (0 <= currentLeaderIndex && currentLeaderIndex < leaderElectionServices.size());

        TestingLeaderElectionService testingLeaderElectionService =
                leaderElectionServices.get(currentLeaderIndex);

        testingLeaderElectionService.notLeader();

        currentLeaderIndex = -1;
        currentLeaderId = null;
    }

    public void notifyRetrievers(int index, UUID leaderId) {
        for (SettableLeaderRetrievalService retrievalService : leaderRetrievalServices) {
            retrievalService.notifyListener(getLeaderAddress(index), leaderId);
        }
    }

    private String getLeaderAddress(int index) {
        if (0 <= index && index < leaderElectionServices.size()) {
            TestingLeaderElectionService testingLeaderElectionService =
                    leaderElectionServices.get(index);
            return testingLeaderElectionService.getAddress();
        } else {
            return null;
        }
    }
}
