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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.ARG_INPUT;
import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.ARG_RESET_TTL_ON_DUPLICATE;
import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.ARG_STATE_TTL;
import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.CANDIDATE_STATE_TYPE_STRATEGY;
import static org.apache.flink.table.types.inference.strategies.DeduplicateKeepFirstTypeStrategy.SEEN_STATE_TYPE_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the state contract of {@link DeduplicateKeepFirstTypeStrategy}. */
class DeduplicateKeepFirstTypeStrategyTest {

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("user_name", DataTypes.STRING()),
                    DataTypes.FIELD("action", DataTypes.STRING()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)));

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
    void testCandidateStateWithoutOnTimeIsFixed() {
        assertThat(CANDIDATE_STATE_TYPE_STRATEGY.inferType(callContext(-1)))
                .contains(ValueView.newValueViewDataType(DataTypes.BOOLEAN()));
    }

    @Test
    void testCandidateStateWithOnTimeNestsPayload() {
        assertThat(CANDIDATE_STATE_TYPE_STRATEGY.inferType(callContext(2)))
                .contains(
                        ValueView.newValueViewDataType(
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "payload",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "action", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "ts", DataTypes.TIMESTAMP_LTZ(3)))),
                                        DataTypes.FIELD("event_time", DataTypes.BIGINT()))));
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

    private static CallContextMock callContext(final int timeColumn) {
        final CallContextMock callContext = new CallContextMock();
        callContext.tableSemantics =
                Map.of(
                        ARG_INPUT,
                        new TableSemanticsMock(
                                INPUT_TYPE, new int[] {0}, new int[0], timeColumn, null));
        return callContext;
    }
}
