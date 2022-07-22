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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

/** Factory for a {@link MultipleComponentLeaderElectionDriverAdapter}. */
final class MultipleComponentLeaderElectionDriverAdapterFactory
        implements LeaderElectionDriverFactory {
    private final String leaderName;
    private final MultipleComponentLeaderElectionService singleLeaderElectionService;

    MultipleComponentLeaderElectionDriverAdapterFactory(
            String leaderName, MultipleComponentLeaderElectionService singleLeaderElectionService) {
        this.leaderName = Preconditions.checkNotNull(leaderName);
        this.singleLeaderElectionService = Preconditions.checkNotNull(singleLeaderElectionService);
    }

    @Override
    public LeaderElectionDriver createLeaderElectionDriver(
            LeaderElectionEventHandler leaderEventHandler,
            FatalErrorHandler fatalErrorHandler,
            String leaderContenderDescription)
            throws Exception {
        return new MultipleComponentLeaderElectionDriverAdapter(
                leaderName, singleLeaderElectionService, leaderEventHandler);
    }
}
