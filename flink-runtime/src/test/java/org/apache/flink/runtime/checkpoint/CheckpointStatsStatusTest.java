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

package org.apache.flink.runtime.checkpoint;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointStatsStatusTest {

    /** Tests the getters of each status. */
    @Test
    void testStatusValues() {
        CheckpointStatsStatus inProgress = CheckpointStatsStatus.IN_PROGRESS;
        assertThat(inProgress.isInProgress()).isTrue();
        assertThat(inProgress.isCompleted()).isFalse();
        assertThat(inProgress.isFailed()).isFalse();

        CheckpointStatsStatus completed = CheckpointStatsStatus.COMPLETED;
        assertThat(completed.isInProgress()).isFalse();
        assertThat(completed.isCompleted()).isTrue();
        assertThat(completed.isFailed()).isFalse();

        CheckpointStatsStatus failed = CheckpointStatsStatus.FAILED;
        assertThat(failed.isInProgress()).isFalse();
        assertThat(failed.isCompleted()).isFalse();
        assertThat(failed.isFailed()).isTrue();
    }
}
