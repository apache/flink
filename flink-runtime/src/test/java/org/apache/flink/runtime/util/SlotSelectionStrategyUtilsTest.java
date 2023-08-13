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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PreviousAllocationSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SlotSelectionStrategyUtils}. */
class SlotSelectionStrategyUtilsTest {

    @Test
    void testCreatePreviousAllocationSlotSelectionStrategyForLocalRecoveryStreamingJob() {
        final Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.LOCAL_RECOVERY, true);

        final SlotSelectionStrategy slotSelectionStrategy =
                SlotSelectionStrategyUtils.selectSlotSelectionStrategy(
                        JobType.STREAMING, configuration);

        assertThat(slotSelectionStrategy)
                .isInstanceOf(PreviousAllocationSlotSelectionStrategy.class);
    }

    @Test
    void testCreateLocationPreferenceSlotSelectionStrategyForLocalRecoveryBatchJob() {
        final Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.LOCAL_RECOVERY, true);

        final SlotSelectionStrategy slotSelectionStrategy =
                SlotSelectionStrategyUtils.selectSlotSelectionStrategy(
                        JobType.BATCH, configuration);

        assertThat(slotSelectionStrategy)
                .isInstanceOf(LocationPreferenceSlotSelectionStrategy.class);
    }
}
