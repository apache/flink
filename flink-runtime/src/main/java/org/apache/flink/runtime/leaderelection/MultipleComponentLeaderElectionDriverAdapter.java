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

import org.apache.flink.util.Preconditions;

/**
 * {@link LeaderElectionDriver} adapter that multiplexes the leader election of a component into a
 * single leader election via {@link MultipleComponentLeaderElectionService}.
 */
final class MultipleComponentLeaderElectionDriverAdapter implements LeaderElectionDriver {
    private final String componentId;
    private final MultipleComponentLeaderElectionService multipleComponentLeaderElectionService;

    MultipleComponentLeaderElectionDriverAdapter(
            String componentId,
            MultipleComponentLeaderElectionService multipleComponentLeaderElectionService,
            LeaderElectionEventHandler leaderElectionEventHandler) {
        this.componentId = Preconditions.checkNotNull(componentId);
        this.multipleComponentLeaderElectionService =
                Preconditions.checkNotNull(multipleComponentLeaderElectionService);

        multipleComponentLeaderElectionService.registerLeaderElectionEventHandler(
                this.componentId, leaderElectionEventHandler);
    }

    @Override
    public void writeLeaderInformation(LeaderInformation leaderInformation) {
        multipleComponentLeaderElectionService.publishLeaderInformation(
                componentId, leaderInformation);
    }

    @Override
    public boolean hasLeadership() {
        return multipleComponentLeaderElectionService.hasLeadership(componentId);
    }

    @Override
    public void close() throws Exception {
        multipleComponentLeaderElectionService.unregisterLeaderElectionEventHandler(componentId);
    }
}
