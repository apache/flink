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

import org.apache.flink.util.Preconditions;

import java.util.UUID;

/** {@code LeaderElectionUtils} collects helper methods to handle LeaderElection-related issues. */
public class LeaderElectionUtils {

    /**
     * Converts the passed {@link LeaderInformation} into a human-readable representation that can
     * be used in log messages.
     */
    public static String convertToString(LeaderInformation leaderInformation) {
        return leaderInformation.isEmpty()
                ? "<no leader>"
                : convertToString(
                        leaderInformation.getLeaderSessionID(),
                        leaderInformation.getLeaderAddress());
    }

    public static String convertToString(UUID sessionId, String address) {
        return String.format(
                "%s@%s",
                Preconditions.checkNotNull(sessionId), Preconditions.checkNotNull(address));
    }
}
