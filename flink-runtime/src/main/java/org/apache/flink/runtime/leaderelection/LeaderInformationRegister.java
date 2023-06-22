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
import java.util.stream.Collectors;

/**
 * A register containing the {@link LeaderInformation} for multiple contenders based on their {@code
 * contenderID}. No empty {@code LeaderInformation} is stored physically. No entry and an entry with
 * an empty {@code LeaderInformation} are, therefore, semantically the same.
 */
public class LeaderInformationRegister {

    private static final LeaderInformationRegister EMPTY_REGISTER =
            new LeaderInformationRegister(Collections.emptyMap());

    private final Map<String, LeaderInformation> leaderInformationPerContenderID;

    public static LeaderInformationRegister empty() {
        return EMPTY_REGISTER;
    }

    /** Creates a single-entry instance containing only the passed information. */
    public static LeaderInformationRegister of(
            String contenderID, LeaderInformation leaderInformation) {
        return new LeaderInformationRegister(
                Collections.singletonMap(contenderID, leaderInformation));
    }

    /**
     * Merges another {@code LeaderInformationRegister} with additional leader information into a
     * new {@code LeaderInformationRegister} instance. Any existing {@link LeaderInformation} for
     * the passed {@code contenderID} will be overwritten.
     *
     * <p>Empty {@code LeaderInformation} results in the removal of the corresponding entry (if it
     * exists).
     */
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

    /**
     * Creates a new {@code LeaderInformationRegister} that matches the passed {@code
     * LeaderInformationRegister} except for the entry of {@code contenderID} which is removed if it
     * existed.
     */
    public static LeaderInformationRegister clear(
            @Nullable LeaderInformationRegister leaderInformationRegister, String contenderID) {
        if (leaderInformationRegister == null
                || !leaderInformationRegister.getRegisteredContenderIDs().iterator().hasNext()) {
            return LeaderInformationRegister.empty();
        }

        return merge(leaderInformationRegister, contenderID, LeaderInformation.empty());
    }

    /** Creates a {@code LeaderInformationRegister} based on the passed leader information. */
    public LeaderInformationRegister(
            Map<String, LeaderInformation> leaderInformationPerContenderID) {
        this.leaderInformationPerContenderID =
                leaderInformationPerContenderID.entrySet().stream()
                        .filter(entry -> !entry.getValue().isEmpty())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Returns the {@link LeaderInformation} that is stored or an empty {@code Optional} if no entry
     * exists for the passed {@code contenderID}.
     */
    public Optional<LeaderInformation> forContenderID(String contenderID) {
        return Optional.ofNullable(leaderInformationPerContenderID.get(contenderID));
    }

    /**
     * Returns a {@link LeaderInformation} which is empty if no {@code LeaderInformation} is stored
     * for the passed {@code contenderID}.
     */
    public LeaderInformation forContenderIdOrEmpty(String contenderID) {
        return forContenderID(contenderID).orElse(LeaderInformation.empty());
    }

    /** Returns the {@code contenderID}s for which leader information is stored. */
    public Iterable<String> getRegisteredContenderIDs() {
        return leaderInformationPerContenderID.keySet();
    }

    /**
     * Checks whether the register holds non-empty {@link LeaderInformation} for the passed {@code
     * contenderID}.
     */
    public boolean hasLeaderInformation(String contenderID) {
        return leaderInformationPerContenderID.containsKey(contenderID);
    }

    /**
     * Checks that no non-empty {@link LeaderInformation} is stored.
     *
     * @return {@code true}, if there is no entry that refers to a non-empty {@code
     *     LeaderInformation}; otherwise {@code false} (i.e. either no information is stored under
     *     any {@code contenderID} or there are entries for certain {@code contenderID}s that refer
     *     to an empty {@code LeaderInformation} record).
     */
    public boolean hasNoLeaderInformation() {
        return leaderInformationPerContenderID.isEmpty();
    }
}
