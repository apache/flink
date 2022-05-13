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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.UUID;

/**
 * {@link LeaderRetrievalService} implementation which directly forwards calls of notifyListener to
 * the listener.
 */
public class SettableLeaderRetrievalService implements LeaderRetrievalService {

    private String leaderAddress;
    private UUID leaderSessionID;

    private LeaderRetrievalListener listener;

    public SettableLeaderRetrievalService() {
        this(null, null);
    }

    public SettableLeaderRetrievalService(
            @Nullable String leaderAddress, @Nullable UUID leaderSessionID) {
        this.leaderAddress = leaderAddress;
        this.leaderSessionID = leaderSessionID;
    }

    @Override
    public synchronized void start(LeaderRetrievalListener listener) throws Exception {
        this.listener = Preconditions.checkNotNull(listener);

        if (leaderSessionID != null && leaderAddress != null) {
            listener.notifyLeaderAddress(leaderAddress, leaderSessionID);
        }
    }

    @Override
    public void stop() throws Exception {}

    public synchronized void notifyListener(
            @Nullable String address, @Nullable UUID leaderSessionID) {
        this.leaderAddress = address;
        this.leaderSessionID = leaderSessionID;

        if (listener != null) {
            listener.notifyLeaderAddress(address, leaderSessionID);
        }
    }
}
