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

package org.apache.flink.runtime.util;

import java.util.UUID;

/** Wrapper class for a pair of connection address and leader session ID. */
public class LeaderConnectionInfo {
    private final UUID leaderSessionId;

    private final String address;

    public LeaderConnectionInfo(UUID leaderSessionId, String address) {
        this.leaderSessionId = leaderSessionId;
        this.address = address;
    }

    public UUID getLeaderSessionId() {
        return leaderSessionId;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return "LeaderConnectionInfo{"
                + "leaderSessionId="
                + leaderSessionId
                + ", address='"
                + address
                + '\''
                + '}';
    }
}
