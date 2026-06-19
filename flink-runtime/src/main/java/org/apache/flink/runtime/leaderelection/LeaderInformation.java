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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Information about leader including the confirmed leader session id and leader address. */
public class LeaderInformation implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final UUID leaderSessionID;

    @Nullable private final String leaderAddress;

    private static final LeaderInformation EMPTY = new LeaderInformation(null, null);

    private LeaderInformation(@Nullable UUID leaderSessionID, @Nullable String leaderAddress) {
        this.leaderSessionID = leaderSessionID;
        this.leaderAddress = leaderAddress;
    }

    @Nullable
    public UUID getLeaderSessionID() {
        return leaderSessionID;
    }

    @Nullable
    public String getLeaderAddress() {
        return leaderAddress;
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == LeaderInformation.class) {
            final LeaderInformation that = (LeaderInformation) obj;
            return Objects.equals(this.leaderSessionID, that.leaderSessionID)
                    && Objects.equals(this.leaderAddress, that.leaderAddress);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(leaderSessionID);
        result = 31 * result + Objects.hashCode(leaderAddress);
        return result;
    }

    public static LeaderInformation known(UUID leaderSessionID, String leaderAddress) {
        return new LeaderInformation(checkNotNull(leaderSessionID), checkNotNull(leaderAddress));
    }

    public static LeaderInformation empty() {
        return EMPTY;
    }

    @Override
    public String toString() {
        return "LeaderInformation{"
                + "leaderSessionID='"
                + leaderSessionID
                + '\''
                + ", leaderAddress="
                + leaderAddress
                + '}';
    }
}
