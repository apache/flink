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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.base.source.reader.mocks.MockRecordEmitter;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;

import static org.apache.flink.metrics.testutils.MetricAssertions.assertThatCounter;
import static org.apache.flink.metrics.testutils.MetricAssertions.assertThatGauge;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests whether all provided metrics of a {@link Source} are of the expected values (FLIP-33). */
public class SourceMetricsITCase extends TestLogger {
    private static final int DEFAULT_PARALLELISM = 4;
    // since integration tests depend on wall clock time, use huge lags
    private static final long EVENTTIME_LAG = Duration.ofDays(100).toMillis();
    private static final long WATERMARK_LAG = Duration.ofDays(1).toMillis();
    private static final long EVENTTIME_EPSILON = Duration.ofDays(20).toMillis();
    // this basically is the time a build is allowed to be frozen before the test fails
    private static final long WATERMARK_EPSILON = Duration.ofHours(6).toMillis();
    @Rule public final SharedObjects sharedObjects = SharedObjects.create();
    private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setConfiguration(reporter.addToConfiguration(new Configuration()))
                            .build());

    @Test
    public void testMetricsWithTimestamp() throws Exception {
        long baseTime = System.currentTimeMillis() - EVENTTIME_LAG;
        WatermarkStrategy<Integer> strategy =
                WatermarkStrategy.forGenerator(
                                context -> new EagerBoundedOutOfOrdernessWatermarks())
                        .withTimestampAssigner(new LaggingTimestampAssigner(baseTime));

        testMetrics(strategy, true);
    }

    @Test
    public void testMetricsWithoutTimestamp() throws Exception {
        testMetrics(WatermarkStrategy.noWatermarks(), false);
    }

    private void testMetrics(WatermarkStrategy<Integer> strategy, boolean hasTimestamps)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int numSplits = Math.max(1, env.getParallelism() - 2);
        env.getConfig().setAutoWatermarkInterval(1L);

        int numRecordsPerSplit = 10;
        MockBaseSource source =
                new MockBaseSource(numSplits, numRecordsPerSplit, Boundedness.BOUNDED);

        // make sure all parallel instances have processed the same amount of records before
        // validating metrics
        SharedReference<CyclicBarrier> beforeBarrier =
                sharedObjects.add(new CyclicBarrier(numSplits + 1));
        SharedReference<CyclicBarrier> afterBarrier =
                sharedObjects.add(new CyclicBarrier(numSplits + 1));
        int stopAtRecord1 = 3;
        int stopAtRecord2 = numRecordsPerSplit - 1;
        DataStream<Integer> stream =
                env.fromSource(source, strategy, "MetricTestingSource")
                        .map(
                                i -> {
                                    if (i % numRecordsPerSplit == stopAtRecord1
                                            || i % numRecordsPerSplit == stopAtRecord2) {
                                        beforeBarrier.get().await();
                                        afterBarrier.get().await();
                                    }
                                    return i;
                                });
        stream.sinkTo(new DiscardingSink<>());
        JobClient jobClient = env.executeAsync();
        final JobID jobId = jobClient.getJobID();

        beforeBarrier.get().await();
        assertSourceMetrics(
                jobId,
                reporter,
                stopAtRecord1 + 1,
                numRecordsPerSplit,
                env.getParallelism(),
                numSplits,
                hasTimestamps);
        afterBarrier.get().await();

        beforeBarrier.get().await();
        assertSourceMetrics(
                jobId,
                reporter,
                stopAtRecord2 + 1,
                numRecordsPerSplit,
                env.getParallelism(),
                numSplits,
                hasTimestamps);
        afterBarrier.get().await();

        jobClient.getJobExecutionResult().get();
    }

    private void assertSourceMetrics(
            JobID jobId,
            InMemoryReporter reporter,
            long processedRecordsPerSubtask,
            long numTotalPerSubtask,
            int parallelism,
            int numSplits,
            boolean hasTimestamps) {
        List<OperatorMetricGroup> groups =
                reporter.findOperatorMetricGroups(jobId, "MetricTestingSource");
        assertThat(groups).hasSize(parallelism);

        int subtaskWithMetrics = 0;
        for (OperatorMetricGroup group : groups) {
            Map<String, Metric> metrics = reporter.getMetricsByGroup(group);
            // there are only 2 splits assigned; so two groups will not update metrics
            if (group.getIOMetricGroup().getNumRecordsInCounter().getCount() == 0) {
                // assert that optional metrics are not initialized when no split assigned
                assertThatGauge(metrics.get(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG))
                        .isEqualTo(InternalSourceReaderMetricGroup.UNDEFINED);
                assertThat(metrics.get(MetricNames.WATERMARK_LAG)).isNull();
                continue;
            }
            subtaskWithMetrics++;
            // I/O metrics

            assertThatCounter(group.getIOMetricGroup().getNumRecordsInCounter())
                    .isEqualTo(processedRecordsPerSubtask);
            assertThatCounter(group.getIOMetricGroup().getNumBytesInCounter())
                    .isEqualTo(processedRecordsPerSubtask * MockRecordEmitter.RECORD_SIZE_IN_BYTES);
            assertThatCounter(group.getIOMetricGroup().getNumRecordsOutCounter())
                    .isEqualTo(processedRecordsPerSubtask);
            assertThatCounter(group.getIOMetricGroup().getNumBytesOutCounter())
                    .isEqualTo(processedRecordsPerSubtask * MockRecordEmitter.RECORD_SIZE_IN_BYTES);
            // MockRecordEmitter is just incrementing errors every even record
            assertThatCounter(metrics.get(MetricNames.NUM_RECORDS_IN_ERRORS))
                    .isEqualTo(processedRecordsPerSubtask / 2);
            if (hasTimestamps) {
                // Timestamp assigner subtracting EVENTTIME_LAG from wall clock
                assertThatGauge(metrics.get(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG))
                        .isCloseTo(EVENTTIME_LAG, EVENTTIME_EPSILON);
                // Watermark is derived from timestamp, so it has to be in the same order of
                // magnitude
                assertThatGauge(metrics.get(MetricNames.WATERMARK_LAG))
                        .isCloseTo(EVENTTIME_LAG, EVENTTIME_EPSILON);
                // Calculate the additional watermark lag (on top of event time lag)
                Long watermarkLag =
                        ((Gauge<Long>) metrics.get(MetricNames.WATERMARK_LAG)).getValue()
                                - ((Gauge<Long>)
                                                metrics.get(
                                                        MetricNames.CURRENT_EMIT_EVENT_TIME_LAG))
                                        .getValue();
                // That should correspond to the out-of-order boundedness
                assertThat(watermarkLag)
                        .isGreaterThan(WATERMARK_LAG - WATERMARK_EPSILON)
                        .isLessThan(WATERMARK_LAG + WATERMARK_EPSILON);
            } else {
                // assert that optional metrics are not initialized when no timestamp assigned
                assertThatGauge(metrics.get(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG))
                        .isEqualTo(InternalSourceReaderMetricGroup.UNDEFINED);
                assertThat(metrics.get(MetricNames.WATERMARK_LAG)).isNull();
            }

            long pendingRecords = numTotalPerSubtask - processedRecordsPerSubtask;
            assertThatGauge(metrics.get(MetricNames.PENDING_RECORDS)).isEqualTo(pendingRecords);
            assertThatGauge(metrics.get(MetricNames.PENDING_BYTES))
                    .isEqualTo(pendingRecords * MockRecordEmitter.RECORD_SIZE_IN_BYTES);
            // test is keeping source idle time metric busy with the barrier
            assertThatGauge(metrics.get(MetricNames.SOURCE_IDLE_TIME)).isEqualTo(0L);
        }
        assertThat(subtaskWithMetrics).isEqualTo(numSplits);
    }

    private static class LaggingTimestampAssigner
            implements SerializableTimestampAssigner<Integer> {
        private final long baseTime;

        public LaggingTimestampAssigner(long baseTime) {
            this.baseTime = baseTime;
        }

        @Override
        public long extractTimestamp(Integer i, long ts) {
            return baseTime + i;
        }
    }

    /** Emits watermarks on each record. */
    private static class EagerBoundedOutOfOrdernessWatermarks
            extends BoundedOutOfOrdernessWatermarks<Integer> {
        public EagerBoundedOutOfOrdernessWatermarks() {
            super(Duration.ofMillis(SourceMetricsITCase.WATERMARK_LAG));
        }

        @Override
        public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
            super.onEvent(event, eventTimestamp, output);
            onPeriodicEmit(output);
        }
    }
}
