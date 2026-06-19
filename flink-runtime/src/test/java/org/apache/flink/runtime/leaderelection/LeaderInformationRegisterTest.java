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
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final LeaderInformationRegister testInstance =
                LeaderInformationRegister.of(componentId, leaderInformation);
        assertThat(testInstance.getRegisteredComponentIds()).containsExactly(componentId);
        assertThat(testInstance.forComponentId(componentId)).hasValue(leaderInformation);
    }

    @Test
    void testOfWithEmptyLeaderInformation() {
        final String componentId = "component-id";
        final LeaderInformationRegister testInstance =
                LeaderInformationRegister.of(componentId, LeaderInformation.empty());

        assertThat(testInstance.getRegisteredComponentIds()).isEmpty();
        assertThat(testInstance.forComponentId(componentId)).isNotPresent();
    }

    @Test
    void testMerge() {
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final String newComponentId = "new-component-id";
        final LeaderInformation newLeaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "new-address");

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(componentId, leaderInformation);

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.merge(
                        initialRegister, newComponentId, newLeaderInformation);

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredComponentIds())
                .containsExactlyInAnyOrder(componentId, newComponentId);
        assertThat(newRegister.forComponentId(componentId)).hasValue(leaderInformation);
        assertThat(newRegister.forComponentId(newComponentId)).hasValue(newLeaderInformation);
    }

    @Test
    void testMergeEmptyLeaderInformation() {
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final String newComponentId = "new-component-id";

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(componentId, leaderInformation);

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.merge(
                        initialRegister, newComponentId, LeaderInformation.empty());

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredComponentIds()).containsExactly(componentId);
        assertThat(newRegister.forComponentId(newComponentId)).isNotPresent();
    }

    @Test
    void testMergeEmptyLeaderInformationForExistingComponentId() {
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(componentId, leaderInformation);

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.merge(
                        initialRegister, componentId, LeaderInformation.empty());

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredComponentIds()).isEmpty();
        assertThat(newRegister.forComponentId(componentId)).isNotPresent();
    }

    @Test
    void testMergeKnownLeaderInformationForExistingComponentId() {
        final String componentId = "component-id";
        final LeaderInformation oldLeaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "old-address");

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(componentId, oldLeaderInformation);

        final LeaderInformation newLeaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "new-address");
        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.merge(initialRegister, componentId, newLeaderInformation);

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredComponentIds()).containsExactly(componentId);
        assertThat(newRegister.forComponentId(componentId)).hasValue(newLeaderInformation);
    }

    @Test
    void testClear() {
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(componentId, leaderInformation);

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.clear(initialRegister, componentId);

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredComponentIds()).isEmpty();
        assertThat(newRegister.forComponentId(componentId)).isNotPresent();
    }

    @Test
    void testClearNotRegisteredComponentId() {
        final String componentId = "component-id";
        final LeaderInformationRegister initialRegister =
                LeaderInformationRegister.of(
                        componentId, LeaderInformation.known(UUID.randomUUID(), "address"));

        final LeaderInformationRegister newRegister =
                LeaderInformationRegister.clear(initialRegister, "another-component-id");

        assertThat(newRegister).isNotSameAs(initialRegister);
        assertThat(newRegister.getRegisteredComponentIds()).containsExactly(componentId);
    }

    @Test
    void testEmptyLeaderInformationFiltering() {
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");

        final String componentIdWithEmptyLeaderInformation = "new-component-id";

        final Map<String, LeaderInformation> records = new HashMap<>();
        records.put(componentId, leaderInformation);
        records.put(componentIdWithEmptyLeaderInformation, LeaderInformation.empty());

        final LeaderInformationRegister testInstance = new LeaderInformationRegister(records);
        assertThat(testInstance.getRegisteredComponentIds()).containsExactly(componentId);
        assertThat(testInstance.forComponentId(componentIdWithEmptyLeaderInformation))
                .isNotPresent();
    }

    @Test
    void testEmptyInstance() {
        assertThat(LeaderInformationRegister.empty().getRegisteredComponentIds()).isEmpty();
    }

    @Test
    void testForComponentId() {
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");
        assertThat(
                        LeaderInformationRegister.of(componentId, leaderInformation)
                                .forComponentId(componentId))
                .hasValue(leaderInformation);
    }

    @Test
    void testForComponentIdOrEmpty() {
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");
        assertThat(
                        LeaderInformationRegister.of(componentId, leaderInformation)
                                .forComponentIdOrEmpty(componentId))
                .isEqualTo(leaderInformation);
    }

    @Test
    void testForComponentIdWithEmptyLeaderInformation() {
        final String componentId = "component-id";
        assertThat(
                        LeaderInformationRegister.of(componentId, LeaderInformation.empty())
                                .forComponentId(componentId))
                .isNotPresent();
    }

    @Test
    void testForComponentIdOrEmptyWithEmptyLeaderInformation() {
        final String componentId = "component-id";
        assertThat(
                        LeaderInformationRegister.of(componentId, LeaderInformation.empty())
                                .forComponentIdOrEmpty(componentId))
                .isEqualTo(LeaderInformation.empty());
    }

    @Test
    void testHasLeaderInformation() {
        final String componentId = "component-id";
        final LeaderInformation leaderInformation =
                LeaderInformation.known(UUID.randomUUID(), "address");
        assertThat(
                        LeaderInformationRegister.of(componentId, leaderInformation)
                                .hasLeaderInformation(componentId))
                .isTrue();
    }

    @Test
    void testHasLeaderInformationWithEmptyLeaderInformation() {
        final String componentId = "component-id";
        assertThat(
                        LeaderInformationRegister.of(componentId, LeaderInformation.empty())
                                .hasLeaderInformation(componentId))
                .isFalse();
    }

    @Test
    void hasNoLeaderInformation() {
        LeaderInformationRegister register = LeaderInformationRegister.empty();
        assertThat(register.hasNoLeaderInformation()).isTrue();

        register = LeaderInformationRegister.of("component-id", LeaderInformation.empty());
        assertThat(register.hasNoLeaderInformation()).isTrue();

        register =
                LeaderInformationRegister.merge(
                        register,
                        "other-component-id",
                        LeaderInformation.known(UUID.randomUUID(), "address"));
        assertThat(register.hasNoLeaderInformation()).isFalse();
    }
}
