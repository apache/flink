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

/** Tests for the {@link CheckpointMetrics} class. */
class CheckpointMetricsTest {

    @Test
    void testUnalignedCheckpointType() {
        CheckpointMetricsBuilder metricsBuilder = new CheckpointMetricsBuilder();
        metricsBuilder.setBytesProcessedDuringAlignment(0L);
        metricsBuilder.setAlignmentDurationNanos(0L);
        metricsBuilder.setBytesPersistedOfThisCheckpoint(0L);
        metricsBuilder.setTotalBytesPersisted(0L);

        // The checkpoint shouldn't be unaligned checkpoint when builder doesn't set
        // bytesPersistedDuringAlignment
        assertThat(metricsBuilder.build().getUnalignedCheckpoint()).isFalse();
        assertThat(metricsBuilder.buildIncomplete().getUnalignedCheckpoint()).isFalse();

        assertUnalignedCheckpointType(metricsBuilder, 0L);
        assertUnalignedCheckpointType(metricsBuilder, 1L);
        assertUnalignedCheckpointType(metricsBuilder, 5L);
        assertUnalignedCheckpointType(metricsBuilder, 10L);
        assertUnalignedCheckpointType(metricsBuilder, 100L);
    }

    private void assertUnalignedCheckpointType(
            CheckpointMetricsBuilder metricsBuilder, long bytesPersistedDuringAlignment) {
        boolean expectedUnalignedCheckpointType = bytesPersistedDuringAlignment > 0;
        metricsBuilder.setBytesPersistedDuringAlignment(bytesPersistedDuringAlignment);

        assertThat(metricsBuilder.build().getUnalignedCheckpoint())
                .isEqualTo(expectedUnalignedCheckpointType);
        assertThat(metricsBuilder.buildIncomplete().getUnalignedCheckpoint())
                .isEqualTo(expectedUnalignedCheckpointType);
    }
}
