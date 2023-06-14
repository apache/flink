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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * A register containing the {@link LeaderInformation} for multiple contenders based on their {@code contenderID}.
 */
public class LeaderInformationRegister {

    private final Map<String, LeaderInformation> leaderInformationPerContenderID;

    public static LeaderInformationRegister of(String contenderID, LeaderInformation leaderInformation) {
        return new LeaderInformationRegister(Collections.singletonMap(contenderID, leaderInformation));
    }

    public LeaderInformationRegister(Map<String, LeaderInformation> leaderInformationPerContenderID) {
        this.leaderInformationPerContenderID = leaderInformationPerContenderID;
    }

    public Optional<LeaderInformation> forContenderID(String contenderID) {
        return Optional.ofNullable(leaderInformationPerContenderID.get(contenderID));
    }

    public Iterable<String> getRegisteredContenderIDs() {
        return leaderInformationPerContenderID.keySet();
    }
}
