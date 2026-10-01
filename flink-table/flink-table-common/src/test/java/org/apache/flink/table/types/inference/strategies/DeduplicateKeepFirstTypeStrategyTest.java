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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.types.inference.utils.CallContextMock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.ARG_RESET_TTL_ON_DUPLICATE;
import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.ARG_STATE_TTL;
import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.CANDIDATE_STATE_TYPE_STRATEGY;
import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.SEEN_STATE_TYPE_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the state retention contract of {@link DeduplicateKeepFirstTypeStrategy}. */
class DeduplicateKeepFirstTypeStrategyTest {

    @Test
    void testCandidateStateDisablesTtl() {
        // The event-time candidate buffer is cleared by its timer, so it must declare a disabled
        // TTL rather than inheriting the global state TTL and expiring before the timer fires.
        assertThat(CANDIDATE_STATE_TYPE_STRATEGY.getTimeToLive(callContext(Duration.ofSeconds(5))))
                .contains(Duration.ZERO);
        assertThat(CANDIDATE_STATE_TYPE_STRATEGY.getTimeToLive(callContext(null)))
                .contains(Duration.ZERO);
    }

    @Test
    void testSeenStateTtlFollowsStateTtlArgument() {
        assertThat(SEEN_STATE_TYPE_STRATEGY.getTimeToLive(callContext(Duration.ofSeconds(5))))
                .contains(Duration.ofSeconds(5));
        assertThat(SEEN_STATE_TYPE_STRATEGY.getTimeToLive(callContext(null))).isEmpty();
    }

    private static CallContextMock callContext(Duration stateTtl) {
        final CallContextMock callContext = new CallContextMock();
        final List<Optional<?>> values = new ArrayList<>();
        for (int i = 0; i <= ARG_RESET_TTL_ON_DUPLICATE; i++) {
            values.add(Optional.empty());
        }
        values.set(ARG_STATE_TTL, Optional.ofNullable(stateTtl));
        callContext.argumentValues = values;
        return callContext;
    }
}
