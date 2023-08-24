/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PartitionTable}. */
class PartitionTableTest {

    private static final JobID JOB_ID = new JobID();
    private static final ResultPartitionID PARTITION_ID = new ResultPartitionID();

    @Test
    void testEmptyTable() {
        final PartitionTable<JobID> table = new PartitionTable<>();

        // an empty table should always return an empty collection
        Collection<ResultPartitionID> partitionsForNonExistingJob =
                table.stopTrackingPartitions(JOB_ID);
        assertThat(partitionsForNonExistingJob).isNotNull().isEmpty();

        assertThat(table.hasTrackedPartitions(JOB_ID)).isFalse();
    }

    @Test
    void testStartTrackingPartition() {
        final PartitionTable<JobID> table = new PartitionTable<>();

        table.startTrackingPartitions(JOB_ID, Collections.singletonList(PARTITION_ID));

        assertThat(table.hasTrackedPartitions(JOB_ID)).isTrue();
    }

    @Test
    void testStartTrackingZeroPartitionDoesNotMutateState() {
        final PartitionTable<JobID> table = new PartitionTable<>();

        table.startTrackingPartitions(JOB_ID, Collections.emptyList());

        assertThat(table.hasTrackedPartitions(JOB_ID)).isFalse();
    }

    @Test
    void testStopTrackingAllPartitions() {
        final PartitionTable<JobID> table = new PartitionTable<>();

        table.startTrackingPartitions(JOB_ID, Collections.singletonList(PARTITION_ID));

        Collection<ResultPartitionID> storedPartitions = table.stopTrackingPartitions(JOB_ID);
        assertThat(storedPartitions).contains(PARTITION_ID);
        assertThat(table.hasTrackedPartitions(JOB_ID)).isFalse();
    }

    @Test
    void testStopTrackingPartitions() {
        final ResultPartitionID partitionId2 = new ResultPartitionID();
        final PartitionTable<JobID> table = new PartitionTable<>();

        table.startTrackingPartitions(JOB_ID, Collections.singletonList(PARTITION_ID));
        table.startTrackingPartitions(JOB_ID, Collections.singletonList(partitionId2));

        table.stopTrackingPartitions(JOB_ID, Collections.singletonList(partitionId2));
        assertThat(table.hasTrackedPartitions(JOB_ID)).isTrue();

        Collection<ResultPartitionID> storedPartitions = table.stopTrackingPartitions(JOB_ID);
        assertThat(storedPartitions).contains(PARTITION_ID);
        assertThat(table.hasTrackedPartitions(JOB_ID)).isFalse();
    }
}
