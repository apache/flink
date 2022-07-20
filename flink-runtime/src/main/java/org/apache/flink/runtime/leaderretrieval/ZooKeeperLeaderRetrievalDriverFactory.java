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

import org.apache.flink.runtime.rpc.FatalErrorHandler;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

/** {@link LeaderRetrievalDriverFactory} implementation for Zookeeper. */
public class ZooKeeperLeaderRetrievalDriverFactory implements LeaderRetrievalDriverFactory {

    private final CuratorFramework client;

    private final String retrievalPath;

    private final ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
            leaderInformationClearancePolicy;

    public ZooKeeperLeaderRetrievalDriverFactory(
            CuratorFramework client,
            String retrievalPath,
            ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                    leaderInformationClearancePolicy) {
        this.client = client;
        this.retrievalPath = retrievalPath;
        this.leaderInformationClearancePolicy = leaderInformationClearancePolicy;
    }

    @Override
    public ZooKeeperLeaderRetrievalDriver createLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderEventHandler, FatalErrorHandler fatalErrorHandler)
            throws Exception {
        return new ZooKeeperLeaderRetrievalDriver(
                client,
                retrievalPath,
                leaderEventHandler,
                leaderInformationClearancePolicy,
                fatalErrorHandler);
    }
}
