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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A register containing the {@link LeaderInformation} for multiple contenders based on their {@code
 * contenderID}.
 */
public class LeaderInformationRegister {

    private static final LeaderInformationRegister EMPTY_REGISTER =
            new LeaderInformationRegister(Collections.emptyMap());

    private final Map<String, LeaderInformation> leaderInformationPerContenderID;

    public static LeaderInformationRegister empty() {
        return EMPTY_REGISTER;
    }

    public static LeaderInformationRegister of(
            String contenderID, LeaderInformation leaderInformation) {
        return new LeaderInformationRegister(
                Collections.singletonMap(contenderID, leaderInformation));
    }

    public static LeaderInformationRegister merge(
            @Nullable LeaderInformationRegister leaderInformationRegister,
            String contenderID,
            LeaderInformation leaderInformation) {
        final Map<String, LeaderInformation> existingLeaderInformation =
                new HashMap<>(
                        leaderInformationRegister == null
                                ? Collections.emptyMap()
                                : leaderInformationRegister.leaderInformationPerContenderID);
        if (leaderInformation.isEmpty()) {
            existingLeaderInformation.remove(contenderID);
        } else {
            existingLeaderInformation.put(contenderID, leaderInformation);
        }

        return new LeaderInformationRegister(existingLeaderInformation);
    }

    public static LeaderInformationRegister clear(
            @Nullable LeaderInformationRegister leaderInformationRegister, String contenderID) {
        if (leaderInformationRegister == null
                || !leaderInformationRegister.getRegisteredContenderIDs().iterator().hasNext()) {
            return LeaderInformationRegister.empty();
        }

        return merge(leaderInformationRegister, contenderID, LeaderInformation.empty());
    }

    public LeaderInformationRegister(
            Map<String, LeaderInformation> leaderInformationPerContenderID) {
        this.leaderInformationPerContenderID = leaderInformationPerContenderID;
    }

    public Optional<LeaderInformation> forContenderID(String contenderID) {
        return Optional.ofNullable(leaderInformationPerContenderID.get(contenderID));
    }

    public Iterable<String> getRegisteredContenderIDs() {
        return leaderInformationPerContenderID.keySet();
    }
}
