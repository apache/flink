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

package org.apache.flink.runtime.highavailability.nonha.standalone;

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.leaderservice.ClientLeaderServices;

import javax.annotation.concurrent.GuardedBy;

import static org.apache.flink.runtime.highavailability.HighAvailabilityServices.DEFAULT_LEADER_ID;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Non-HA implementation for {@link ClientLeaderServices}. The address to rest endpoint is
 * pre-configured.
 */
public class StandaloneClientLeaderServices implements ClientLeaderServices {
    private final Object lock = new Object();
    private final String restEndpointAddress;

    @GuardedBy("lock")
    private boolean running;

    public StandaloneClientLeaderServices(String restEndpointAddress) {
        this.restEndpointAddress = restEndpointAddress;
        this.running = true;
    }

    @Override
    public LeaderRetrievalService getRestEndpointLeaderRetriever() {
        synchronized (lock) {
            checkState(running, "StandaloneClientLeaderServices has already been closed.");
            return new StandaloneLeaderRetrievalService(restEndpointAddress, DEFAULT_LEADER_ID);
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            running = false;
        }
    }
}
