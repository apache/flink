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

import org.apache.flink.runtime.highavailability.ReusableClientHAServices;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;

import static org.apache.flink.runtime.highavailability.HighAvailabilityServices.DEFAULT_LEADER_ID;

/**
 * Non-HA implementation for {@link ReusableClientHAServices}. The address to web monitor is
 * pre-configured.
 */
public class StandaloneReusableClientHAServices implements ReusableClientHAServices {

    private final LeaderRetriever leaderRetriever;

    private final LeaderRetrievalService leaderRetrievalService;

    public StandaloneReusableClientHAServices(
            String webMonitorAddress, LeaderRetriever leaderRetriever) throws Exception {
        this.leaderRetriever = leaderRetriever;
        this.leaderRetrievalService =
                new StandaloneLeaderRetrievalService(webMonitorAddress, DEFAULT_LEADER_ID);
        startLeaderRetrievers();
    }

    @Override
    public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        return this.leaderRetrievalService;
    }

    private void startLeaderRetrievers() throws Exception {
        this.leaderRetrievalService.start(leaderRetriever);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public LeaderRetriever getLeaderRetriever() {
        return this.leaderRetriever;
    }
}
