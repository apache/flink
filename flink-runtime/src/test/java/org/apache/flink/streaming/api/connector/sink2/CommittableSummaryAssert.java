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

import org.assertj.core.api.AbstractObjectAssert;

/** Custom assertions for {@link CommittableSummary}. */
public class CommittableSummaryAssert
        extends AbstractObjectAssert<CommittableSummaryAssert, CommittableSummary<?>> {

    public CommittableSummaryAssert(CommittableSummary<?> summary) {
        super(summary, CommittableSummaryAssert.class);
    }

    public CommittableSummaryAssert hasSubtaskId(int subtaskId) {
        return returns(subtaskId, CommittableSummary::getSubtaskId);
    }

    public CommittableSummaryAssert hasNumberOfSubtasks(int numberOfSubtasks) {
        return returns(numberOfSubtasks, CommittableSummary::getNumberOfSubtasks);
    }

    public CommittableSummaryAssert hasOverallCommittables(int committableNumber) {
        return returns(committableNumber, CommittableSummary::getNumberOfCommittables);
    }

    public CommittableSummaryAssert hasPendingCommittables(int committableNumber) {
        return returns(committableNumber, CommittableSummary::getNumberOfPendingCommittables);
    }

    public CommittableSummaryAssert hasFailedCommittables(int committableNumber) {
        return returns(committableNumber, CommittableSummary::getNumberOfFailedCommittables);
    }

    public CommittableSummaryAssert hasCheckpointId(long checkpointId) {
        return returns(checkpointId, CommittableSummary::getCheckpointIdOrEOI);
    }
}
