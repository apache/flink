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

package org.apache.flink.streaming.api.connector.sink2;

import org.assertj.core.api.AbstractAssert;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/** Custom assertions for {@link CommittableSummary}. */
public class CommittableSummaryAssert
        extends AbstractAssert<CommittableSummaryAssert, CommittableSummary<?>> {

    public CommittableSummaryAssert(CommittableSummary<?> summary) {
        super(summary, CommittableSummaryAssert.class);
    }

    public CommittableSummaryAssert isEqualTo(CommittableSummary<?> summary) {
        isNotNull();
        assertThat(actual.getSubtaskId()).isEqualTo(summary.getSubtaskId());
        assertThat(actual.getCheckpointId()).isEqualTo(summary.getCheckpointId());
        assertThat(actual.getNumberOfSubtasks()).isEqualTo(summary.getNumberOfSubtasks());
        assertThat(actual.getNumberOfCommittables()).isEqualTo(summary.getNumberOfCommittables());
        assertThat(actual.getNumberOfPendingCommittables())
                .isEqualTo(summary.getNumberOfPendingCommittables());
        assertThat(actual.getNumberOfFailedCommittables())
                .isEqualTo(summary.getNumberOfFailedCommittables());
        return this;
    }

    public CommittableSummaryAssert hasSubtaskId(int subtaskId) {
        isNotNull();
        assertThat(actual.getSubtaskId()).isEqualTo(subtaskId);
        return this;
    }

    public CommittableSummaryAssert hasNumberOfSubtasks(int numberOfSubtasks) {
        isNotNull();
        assertThat(actual.getNumberOfSubtasks()).isEqualTo(numberOfSubtasks);
        return this;
    }

    public CommittableSummaryAssert hasOverallCommittables(int committableNumber) {
        isNotNull();
        assertThat(actual.getNumberOfCommittables()).isEqualTo(committableNumber);
        return this;
    }

    public CommittableSummaryAssert hasPendingCommittables(int committableNumber) {
        isNotNull();
        assertThat(actual.getNumberOfPendingCommittables()).isEqualTo(committableNumber);
        return this;
    }

    public CommittableSummaryAssert hasFailedCommittables(int committableNumber) {
        isNotNull();
        assertThat(actual.getNumberOfFailedCommittables()).isEqualTo(committableNumber);
        return this;
    }

    public CommittableSummaryAssert hasCheckpointId(@Nullable Long checkpointId) {
        isNotNull();
        if (checkpointId == null) {
            assertThat(actual.getCheckpointId()).isEmpty();
        } else {
            assertThat(actual.getCheckpointId()).hasValue(checkpointId);
        }
        return this;
    }
}
