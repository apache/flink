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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/** Tests for the {@link TaskIOMetricGroup}. */
public class TaskIOMetricGroupTest {
    @Test
    public void testTaskIOMetricGroup() throws InterruptedException {
        TaskMetricGroup task = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        TaskIOMetricGroup taskIO = task.getIOMetricGroup();

        // test counter forwarding
        assertNotNull(taskIO.getNumRecordsInCounter());
        assertNotNull(taskIO.getNumRecordsOutCounter());

        Counter c1 = new SimpleCounter();
        c1.inc(32L);
        Counter c2 = new SimpleCounter();
        c2.inc(64L);

        taskIO.reuseRecordsInputCounter(c1);
        taskIO.reuseRecordsOutputCounter(c2);
        assertEquals(32L, taskIO.getNumRecordsInCounter().getCount());
        assertEquals(64L, taskIO.getNumRecordsOutCounter().getCount());

        // test IOMetrics instantiation
        taskIO.getNumBytesInCounter().inc(100L);
        taskIO.getNumBytesOutCounter().inc(250L);
        taskIO.getNumBuffersOutCounter().inc(3L);
        taskIO.getIdleTimeMsPerSecond().markStart();
        taskIO.getSoftBackPressuredTimePerSecond().markStart();
        long softSleepTime = 2L;
        Thread.sleep(softSleepTime);
        taskIO.getIdleTimeMsPerSecond().markEnd();
        taskIO.getSoftBackPressuredTimePerSecond().markEnd();

        long hardSleepTime = 4L;
        taskIO.getHardBackPressuredTimePerSecond().markStart();
        Thread.sleep(hardSleepTime);
        taskIO.getHardBackPressuredTimePerSecond().markEnd();

        IOMetrics io = taskIO.createSnapshot();
        assertEquals(32L, io.getNumRecordsIn());
        assertEquals(64L, io.getNumRecordsOut());
        assertEquals(100L, io.getNumBytesIn());
        assertEquals(250L, io.getNumBytesOut());
        assertEquals(3L, taskIO.getNumBuffersOutCounter().getCount());
        assertThat(taskIO.getIdleTimeMsPerSecond().getCount(), greaterThanOrEqualTo(softSleepTime));
        assertThat(
                taskIO.getSoftBackPressuredTimePerSecond().getCount(),
                greaterThanOrEqualTo(softSleepTime));
        assertThat(
                taskIO.getHardBackPressuredTimePerSecond().getCount(),
                greaterThanOrEqualTo(hardSleepTime));
    }

    @Test
    public void testNumBytesProducedOfPartitionsMetrics() {
        TaskMetricGroup task = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        TaskIOMetricGroup taskIO = task.getIOMetricGroup();

        Counter c1 = new SimpleCounter();
        c1.inc(32L);
        Counter c2 = new SimpleCounter();
        c2.inc(64L);

        IntermediateResultPartitionID resultPartitionID1 = new IntermediateResultPartitionID();
        IntermediateResultPartitionID resultPartitionID2 = new IntermediateResultPartitionID();

        taskIO.registerNumBytesProducedCounterForPartition(resultPartitionID1, c1);
        taskIO.registerNumBytesProducedCounterForPartition(resultPartitionID2, c2);

        Map<IntermediateResultPartitionID, Long> numBytesProducedOfPartitions =
                taskIO.createSnapshot().getNumBytesProducedOfPartitions();

        assertEquals(2, numBytesProducedOfPartitions.size());
        assertEquals(32L, numBytesProducedOfPartitions.get(resultPartitionID1).longValue());
        assertEquals(64L, numBytesProducedOfPartitions.get(resultPartitionID2).longValue());
    }
}
