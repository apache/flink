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
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.io.network.metrics.ResultPartitionBytesCounter;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link TaskIOMetricGroup}. */
class TaskIOMetricGroupTest {
    @Test
    void testTaskIOMetricGroup() throws InterruptedException {
        TaskMetricGroup task = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        TaskIOMetricGroup taskIO = new TaskIOMetricGroup(task, clock);

        // test initializing time
        final long initializationTime = 100L;
        assertThat(taskIO.getTaskInitializationDuration()).isEqualTo(0L);
        taskIO.markTaskInitializationStarted();
        clock.advanceTime(Duration.ofMillis(initializationTime));
        assertThat(taskIO.getTaskInitializationDuration()).isGreaterThan(0L);
        long initializationDuration = taskIO.getTaskInitializationDuration();
        taskIO.markTaskStart();
        assertThat(taskIO.getTaskInitializationDuration()).isEqualTo(initializationDuration);

        taskIO.setEnableBusyTime(true);
        taskIO.markTaskStart();
        final long startTime = clock.absoluteTimeMillis();

        // test initializing time remains unchanged after running
        final long runningTime = 200L;
        clock.advanceTime(Duration.ofMillis(runningTime));
        assertThat(taskIO.getTaskInitializationDuration()).isEqualTo(initializationDuration);

        // test counter forwarding
        assertThat(taskIO.getNumRecordsInCounter()).isNotNull();
        assertThat(taskIO.getNumRecordsOutCounter()).isNotNull();

        Counter c1 = new SimpleCounter();
        c1.inc(32L);
        Counter c2 = new SimpleCounter();
        c2.inc(64L);

        taskIO.reuseRecordsInputCounter(c1);
        taskIO.reuseRecordsOutputCounter(c2);

        assertThat(taskIO.getNumRecordsInCounter().getCount()).isEqualTo(32L);
        assertThat(taskIO.getNumRecordsOutCounter().getCount()).isEqualTo(64L);

        // test IOMetrics instantiation
        taskIO.getNumBytesInCounter().inc(100L);
        taskIO.getNumBytesOutCounter().inc(250L);
        taskIO.getNumBuffersOutCounter().inc(3L);
        taskIO.getIdleTimeMsPerSecond().markStart();
        taskIO.getSoftBackPressuredTimePerSecond().markStart();
        long softSleepTime = 2L;
        clock.advanceTime(Duration.ofMillis(softSleepTime));
        taskIO.getIdleTimeMsPerSecond().markEnd();
        taskIO.getSoftBackPressuredTimePerSecond().markEnd();

        long hardSleepTime = 4L;
        taskIO.getHardBackPressuredTimePerSecond().markStart();
        clock.advanceTime(Duration.ofMillis(hardSleepTime));
        taskIO.getHardBackPressuredTimePerSecond().markEnd();

        long ioSleepTime = 3L;
        taskIO.getChangelogBusyTimeMsPerSecond().markStart();
        clock.advanceTime(Duration.ofMillis(ioSleepTime));
        taskIO.getChangelogBusyTimeMsPerSecond().markEnd();

        IOMetrics io = taskIO.createSnapshot();
        assertThat(io.getNumRecordsIn()).isEqualTo(32L);
        assertThat(io.getNumRecordsOut()).isEqualTo(64L);
        assertThat(io.getNumBytesIn()).isEqualTo(100L);
        assertThat(io.getNumBytesOut()).isEqualTo(250L);
        assertThat(taskIO.getNumBuffersOutCounter().getCount()).isEqualTo(3L);
        assertThat(taskIO.getIdleTimeMsPerSecond().getAccumulatedCount())
                .isEqualTo(io.getAccumulateIdleTime());
        assertThat(
                        taskIO.getHardBackPressuredTimePerSecond().getAccumulatedCount()
                                + taskIO.getSoftBackPressuredTimePerSecond().getAccumulatedCount())
                .isEqualTo(io.getAccumulateBackPressuredTime());
        assertThat(io.getAccumulateBusyTime())
                .isEqualTo(
                        clock.absoluteTimeMillis()
                                - startTime
                                - io.getAccumulateIdleTime()
                                - io.getAccumulateBackPressuredTime());
        assertThat(taskIO.getIdleTimeMsPerSecond().getCount())
                .isGreaterThanOrEqualTo(softSleepTime);
        assertThat(taskIO.getSoftBackPressuredTimePerSecond().getCount())
                .isGreaterThanOrEqualTo(softSleepTime);
        assertThat(taskIO.getHardBackPressuredTimePerSecond().getCount())
                .isGreaterThanOrEqualTo(hardSleepTime);
        assertThat(taskIO.getChangelogBusyTimeMsPerSecond().getCount())
                .isGreaterThanOrEqualTo(ioSleepTime);
    }

    @Test
    void testConsistencyOfTime() {
        TaskMetricGroup task = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        TaskIOMetricGroup taskIO = new TaskIOMetricGroup(task, clock);
        taskIO.setEnableBusyTime(true);
        taskIO.markTaskStart();
        final long startTime = clock.absoluteTimeMillis();
        long softBackpressureTime = 100L;
        taskIO.getSoftBackPressuredTimePerSecond().markStart();
        clock.advanceTime(Duration.ofMillis(softBackpressureTime));
        taskIO.getSoftBackPressuredTimePerSecond().markEnd();
        assertThat(taskIO.getSoftBackPressuredTimePerSecond().getAccumulatedCount())
                .isGreaterThanOrEqualTo(softBackpressureTime);

        long hardBackpressureTime = 200L;
        taskIO.getHardBackPressuredTimePerSecond().markStart();
        clock.advanceTime(Duration.ofMillis(hardBackpressureTime));
        taskIO.getHardBackPressuredTimePerSecond().markEnd();
        assertThat(taskIO.getHardBackPressuredTimePerSecond().getAccumulatedCount())
                .isGreaterThanOrEqualTo(hardBackpressureTime);

        long changelogBusyTime = 300L;
        taskIO.getChangelogBusyTimeMsPerSecond().markStart();
        clock.advanceTime(Duration.ofMillis(changelogBusyTime));
        taskIO.getChangelogBusyTimeMsPerSecond().markEnd();
        assertThat(taskIO.getChangelogBusyTimeMsPerSecond().getAccumulatedCount())
                .isGreaterThanOrEqualTo(changelogBusyTime);

        long idleTime = 200L;
        taskIO.getIdleTimeMsPerSecond().markStart();
        clock.advanceTime(Duration.ofMillis(idleTime));
        taskIO.getIdleTimeMsPerSecond().markEnd();
        assertThat(taskIO.getIdleTimeMsPerSecond().getAccumulatedCount())
                .isGreaterThanOrEqualTo(idleTime);
        long totalDuration = clock.absoluteTimeMillis() - startTime;

        // busy time = total time - idle time - backpressure time.
        assertThat(taskIO.getAccumulatedBusyTime())
                .isEqualTo(
                        totalDuration
                                - taskIO.getAccumulatedBackPressuredTimeMs()
                                - taskIO.getIdleTimeMsPerSecond().getAccumulatedCount());
    }

    @Test
    void testResultPartitionBytesMetrics() {
        TaskMetricGroup task = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        TaskIOMetricGroup taskIO = task.getIOMetricGroup();

        ResultPartitionBytesCounter c1 = new ResultPartitionBytesCounter(2);
        ResultPartitionBytesCounter c2 = new ResultPartitionBytesCounter(2);

        c1.inc(0, 32L);
        c1.inc(1, 64L);
        c2.incAll(128L);

        IntermediateResultPartitionID resultPartitionID1 = new IntermediateResultPartitionID();
        IntermediateResultPartitionID resultPartitionID2 = new IntermediateResultPartitionID();

        taskIO.registerResultPartitionBytesCounter(resultPartitionID1, c1);
        taskIO.registerResultPartitionBytesCounter(resultPartitionID2, c2);

        Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes =
                taskIO.createSnapshot().getResultPartitionBytes();

        assertThat(resultPartitionBytes).hasSize(2);
        assertThat(resultPartitionBytes.get(resultPartitionID1).getSubpartitionBytes())
                .containsExactly(32L, 64L);
        assertThat(resultPartitionBytes.get(resultPartitionID2).getSubpartitionBytes())
                .containsExactly(128L, 128L);
    }
}
