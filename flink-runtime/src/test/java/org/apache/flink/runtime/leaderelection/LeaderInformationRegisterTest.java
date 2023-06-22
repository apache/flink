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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LeaderInformationRegisterTest {

    @Test
    void testOfWithKnownLeaderInformation() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final LeaderInformationRegister testInstance =
                LeaderInformationRegister.of(contenderID, leaderInformation);
        assertThat(testInstance.getRegisteredContenderIDs()).containsExactly(contenderID);
        assertThat(testInstance.forContenderID(contenderID)).hasValue(leaderInformation);
    }

    @Test
    void testOfWithEmptyLeaderInformation() {
        final String contenderID = "contender-id";
        final LeaderInformationRegister testInstance =
                LeaderInformationRegister.of(contenderID, LeaderInformation.empty());

        assertThat(testInstance.getRegisteredContenderIDs()).isEmpty();
        assertThat(testInstance.forContenderID(contenderID)).isNotPresent();
    }

    @Test
    void testMerge() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final String newContenderID = "new-contender-id";
        final LeaderInformation newLeaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "new-address");

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(contenderID, leaderInformation);

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.merge(
                        initialRegister, newContenderID, newLeaderInformation);

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredContenderIDs())
                .containsExactlyInAnyOrder(contenderID, newContenderID);
        assertThat(newRegister.forContenderID(contenderID)).hasValue(leaderInformation);
        assertThat(newRegister.forContenderID(newContenderID)).hasValue(newLeaderInformation);
    }

    @Test
    void testMergeEmptyLeaderInformation() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final String newContenderID = "new-contender-id";

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(contenderID, leaderInformation);

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.merge(
                        initialRegister, newContenderID, LeaderInformation.empty());

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredContenderIDs()).containsExactly(contenderID);
        assertThat(newRegister.forContenderID(newContenderID)).isNotPresent();
    }

    @Test
    void testMergeEmptyLeaderInformationForExistingContenderID() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(contenderID, leaderInformation);

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.merge(
                        initialRegister, contenderID, LeaderInformation.empty());

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredContenderIDs()).isEmpty();
        assertThat(newRegister.forContenderID(contenderID)).isNotPresent();
    }

    @Test
    void testClear() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(contenderID, leaderInformation);

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.clear(initialRegister, contenderID);

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredContenderIDs()).isEmpty();
        assertThat(newRegister.forContenderID(contenderID)).isNotPresent();
    }

    @Test
    void testClearNotRegisteredContenderID() {
        final String contenderID = "contender-id";
        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(
                        contenderID, LeaderInformation.known(UUID.randomUUID(), "address"));

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.clear(initialRegister, "another-contender-id");

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredContenderIDs()).containsExactly(contenderID);
    }

    @Test
    void testEmptyLeaderInformationFiltering() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final String contenderIDWithEmptyLeaderInformation = "new-contender-id";

        final Map<String, LeaderInformation> records = new HashMap<>();
        records.put(contenderID, leaderInformation);
        records.put(contenderIDWithEmptyLeaderInformation, LeaderInformation.empty());

        final LeaderInformationRegister testInstance = new LeaderInformationRegister(records);
        assertThat(testInstance.getRegisteredContenderIDs()).containsExactly(contenderID);
        assertThat(testInstance.forContenderID(contenderIDWithEmptyLeaderInformation))
                .isNotPresent();
    }

    @Test
    void testEmptyInstance() {
        assertThat(LeaderInformationRegister.empty().getRegisteredContenderIDs()).isEmpty();
    }

    @Test
    void testForContenderID() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");
        assertThat(
                        LeaderInformationRegister.of(contenderID, leaderInformation)
                                .forContenderID(contenderID))
                .hasValue(leaderInformation);
    }

    @Test
    void testForContenderIDOrEmpty() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");
        assertThat(
                        LeaderInformationRegister.of(contenderID, leaderInformation)
                                .forContenderIDOrEmpty(contenderID))
                .isEqualTo(leaderInformation);
    }

    @Test
    void testForContenderIDWithEmptyLeaderInformation() {
        final String contenderID = "contender-id";
        assertThat(
                        LeaderInformationRegister.of(contenderID, LeaderInformation.empty())
                                .forContenderID(contenderID))
                .isNotPresent();
    }

    @Test
    void testForContenderIDOrEmptyWithEmptyLeaderInformation() {
        final String contenderID = "contender-id";
        assertThat(
                        LeaderInformationRegister.of(contenderID, LeaderInformation.empty())
                                .forContenderIDOrEmpty(contenderID))
                .isEqualTo(LeaderInformation.empty());
    }

    @Test
    void testHasLeaderInformation() {
        final String contenderID = "contender-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");
        assertThat(
                        LeaderInformationRegister.of(contenderID, leaderInformation)
                                .hasLeaderInformation(contenderID))
                .isTrue();
    }

    @Test
    void testHasLeaderInformationWithEmptyLeaderInformation() {
        final String contenderID = "contender-id";
        assertThat(
                        LeaderInformationRegister.of(contenderID, LeaderInformation.empty())
                                .hasLeaderInformation(contenderID))
                .isFalse();
    }

    @Test
    void hasNoLeaderInformation() {
        LeaderInformationRegister register = LeaderInformationRegister.empty();
        assertThat(register.hasNoLeaderInformation()).isTrue();

        register = LeaderInformationRegister.of("contender-id", LeaderInformation.empty());
        assertThat(register.hasNoLeaderInformation()).isTrue();

        register =
                LeaderInformationRegister.merge(
                        register,
                        "other-contender-id",
                        LeaderInformation.known(UUID.randomUUID(), "address"));
        assertThat(register.hasNoLeaderInformation()).isFalse();
    }
}
