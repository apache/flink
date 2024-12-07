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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.EpochManager.EpochStatus;
import org.apache.flink.runtime.asyncprocessing.EpochManager.ParallelMode;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for Epoch Manager. */
class EpochManagerTest {
    @Test
    void testBasic() {
        EpochManager epochManager = new EpochManager(null);
        Epoch epoch1 = epochManager.onRecord();
        Epoch epoch2 = epochManager.onRecord();
        assertThat(epoch1).isEqualTo(epoch2);
        assertThat(epoch1.ongoingRecordCount).isEqualTo(2);
        AtomicInteger output = new AtomicInteger(0);
        epochManager.onNonRecord(
                () -> output.incrementAndGet(), ParallelMode.PARALLEL_BETWEEN_EPOCH);
        // record3 is in a new epoch
        Epoch epoch3 = epochManager.onRecord();
        assertThat(epoch3).isNotEqualTo(epoch1);
        assertThat(epochManager.outputQueue.size()).isEqualTo(1);
        assertThat(epochManager.outputQueue.peek().status).isEqualTo(EpochStatus.CLOSED);
        // records in first epoch are not finished, so the non-record action is not executed
        assertThat(output.get()).isEqualTo(0);
        epochManager.completeOneRecord(epoch1);
        epochManager.completeOneRecord(epoch2);
        epochManager.completeOneRecord(epoch3);
        // records in first epoch are finished, so the non-record action is executed
        assertThat(output.get()).isEqualTo(1);
        // records in second epoch are finished, but no newly arrived non-record, so the second
        // epoch is still open
        assertThat(epochManager.outputQueue.size()).isEqualTo(0);
        assertThat(epochManager.activeEpoch.ongoingRecordCount).isEqualTo(0);
        assertThat(epochManager.activeEpoch.status).isEqualTo(EpochStatus.OPEN);
    }
}
