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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.metrics.testutils.MetricAssertions.assertThatCounter;
import static org.apache.flink.metrics.testutils.MetricAssertions.assertThatGauge;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;

/** Tests whether all provided metrics of a {@link Sink} are of the expected values (FLIP-33). */
public class SinkV2MetricsITCase extends TestLogger {

    private static final String TEST_SINK_NAME = "MetricTestSink";
    // please refer to SinkTransformationTranslator#WRITER_NAME
    private static final String DEFAULT_WRITER_NAME = "Writer";
    private static final String DEFAULT_COMMITTER_NAME = "Committer";
    private static final int DEFAULT_PARALLELISM = 4;

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();
    private final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setConfiguration(reporter.addToConfiguration(new Configuration()))
                            .build());

    @Test
    public void testMetrics() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int numSplits = Math.max(1, env.getParallelism() - 2);

        int numRecordsPerSplit = 10;

        // make sure all parallel instances have processed the same amount of records before
        // validating metrics
        SharedReference<CyclicBarrier> beforeBarrier =
                sharedObjects.add(new CyclicBarrier(numSplits + 1));
        SharedReference<CyclicBarrier> afterBarrier =
                sharedObjects.add(new CyclicBarrier(numSplits + 1));
        int stopAtRecord1 = 4;
        int stopAtRecord2 = numRecordsPerSplit - 1;

        env.fromSequence(0, numSplits - 1)
                .<Long>flatMap(
                        (split, collector) ->
                                LongStream.range(0, numRecordsPerSplit).forEach(collector::collect))
                .returns(BasicTypeInfo.LONG_TYPE_INFO)
                .map(
                        i -> {
                            if (i % numRecordsPerSplit == stopAtRecord1
                                    || i % numRecordsPerSplit == stopAtRecord2) {
                                beforeBarrier.get().await();
                                afterBarrier.get().await();
                            }
                            return i;
                        })
                .sinkTo(TestSinkV2.<Long>newBuilder().setWriter(new MetricWriter()).build())
                .name(TEST_SINK_NAME);
        JobClient jobClient = env.executeAsync();
        final JobID jobId = jobClient.getJobID();

        beforeBarrier.get().await();
        assertSinkMetrics(jobId, stopAtRecord1, numSplits);
        afterBarrier.get().await();

        beforeBarrier.get().await();
        assertSinkMetrics(jobId, stopAtRecord2, numSplits);
        afterBarrier.get().await();

        jobClient.getJobExecutionResult().get();
    }

    @Test
    public void testCommitterMetrics() throws Exception {
        final int numCommittables = 7;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make sure all parallel instances have processed the records once before validating
        // metrics
        SharedReference<CountDownLatch> beforeLatch =
                sharedObjects.add(new CountDownLatch(numCommittables));
        SharedReference<CountDownLatch> afterLatch = sharedObjects.add(new CountDownLatch(1));

        env.fromSequence(0, numCommittables - 1)
                .returns(BasicTypeInfo.LONG_TYPE_INFO)
                .sinkTo(
                        TestSinkV2.<Long>newBuilder()
                                .setCommitter(new MetricCommitter(beforeLatch, afterLatch))
                                .setCommittableSerializer(TestSinkV2.StringSerializer.INSTANCE)
                                .build())
                .name(TEST_SINK_NAME);
        JobClient jobClient = env.executeAsync();
        final JobID jobId = jobClient.getJobID();

        // Run until every committer finished with 1 commit round - everything should be retried and
        // pending
        beforeLatch.get().await();
        assertSinkCommitterMetrics(
                jobId,
                ImmutableMap.of(
                        MetricNames.ALREADY_COMMITTED_COMMITTABLES, 0L,
                        MetricNames.FAILED_COMMITTABLES, 0L,
                        MetricNames.RETRIED_COMMITTABLES, 7L,
                        MetricNames.SUCCESSFUL_COMMITTABLES, 0L,
                        MetricNames.TOTAL_COMMITTABLES, 7L,
                        MetricNames.PENDING_COMMITTABLES, 7L));
        afterLatch.get().countDown();

        // Run until finished
        jobClient.getJobExecutionResult().get();
        assertSinkCommitterMetrics(
                jobId,
                ImmutableMap.of(
                        MetricNames.ALREADY_COMMITTED_COMMITTABLES, 1L,
                        MetricNames.FAILED_COMMITTABLES, 2L,
                        MetricNames.RETRIED_COMMITTABLES, 10L,
                        MetricNames.SUCCESSFUL_COMMITTABLES, 4L,
                        MetricNames.TOTAL_COMMITTABLES, 7L,
                        MetricNames.PENDING_COMMITTABLES, 0L));
    }

    @SuppressWarnings("checkstyle:WhitespaceAfter")
    private void assertSinkMetrics(JobID jobId, long processedRecordsPerSubtask, int numSplits) {
        List<OperatorMetricGroup> groups =
                reporter.findOperatorMetricGroups(
                        jobId, TEST_SINK_NAME + ": " + DEFAULT_WRITER_NAME);

        int subtaskWithMetrics = 0;
        for (OperatorMetricGroup group : groups) {
            Map<String, Metric> metrics = reporter.getMetricsByGroup(group);
            // There are only 2 splits assigned; so two groups will not update metrics.
            if (group.getIOMetricGroup() == null
                    || group.getIOMetricGroup().getNumRecordsOutCounter() == null
                    || group.getIOMetricGroup().getNumRecordsOutCounter().getCount() == 0) {
                continue;
            }
            subtaskWithMetrics++;

            // SinkWriterMetricGroup metrics
            assertThatCounter(metrics.get(MetricNames.IO_NUM_RECORDS_OUT))
                    .isEqualTo(processedRecordsPerSubtask);
            assertThatCounter(metrics.get(MetricNames.IO_NUM_BYTES_OUT))
                    .isEqualTo(processedRecordsPerSubtask * MetricWriter.RECORD_SIZE_IN_BYTES);
            // MetricWriter is just incrementing errors every even record
            assertThatCounter(metrics.get(MetricNames.NUM_RECORDS_OUT_ERRORS))
                    .isEqualTo((processedRecordsPerSubtask + 1) / 2);

            // Test "send" metric series has the same value as "out" metric series.
            assertThatCounter(metrics.get(MetricNames.NUM_RECORDS_SEND))
                    .isEqualTo(processedRecordsPerSubtask);
            assertThatCounter(metrics.get(MetricNames.NUM_BYTES_SEND))
                    .isEqualTo(processedRecordsPerSubtask * MetricWriter.RECORD_SIZE_IN_BYTES);
            assertThatCounter(metrics.get(MetricNames.NUM_RECORDS_SEND_ERRORS))
                    .isEqualTo((processedRecordsPerSubtask + 1) / 2);

            // check if the latest send time is fetched
            assertThatGauge(metrics.get(MetricNames.CURRENT_SEND_TIME))
                    .isEqualTo((processedRecordsPerSubtask - 1) * MetricWriter.BASE_SEND_TIME);
        }
        assertThat(subtaskWithMetrics, equalTo(numSplits));
    }

    private void assertSinkCommitterMetrics(JobID jobId, Map<String, Long> expected) {
        List<OperatorMetricGroup> groups =
                reporter.findOperatorMetricGroups(
                        jobId, TEST_SINK_NAME + ": " + DEFAULT_COMMITTER_NAME);

        Map<String, Long> aggregated = new HashMap<>(6);
        for (OperatorMetricGroup group : groups) {
            Map<String, Metric> metrics = reporter.getMetricsByGroup(group);

            for (String metricName :
                    Arrays.asList(
                            MetricNames.SUCCESSFUL_COMMITTABLES,
                            MetricNames.ALREADY_COMMITTED_COMMITTABLES,
                            MetricNames.RETRIED_COMMITTABLES,
                            MetricNames.FAILED_COMMITTABLES,
                            MetricNames.TOTAL_COMMITTABLES)) {
                final Counter counter = (Counter) metrics.get(metricName);
                if (counter != null) {
                    aggregated.merge(metricName, counter.getCount(), Long::sum);
                }
            }

            Gauge<Integer> pendingMetrics =
                    (Gauge<Integer>) metrics.get(MetricNames.PENDING_COMMITTABLES);
            if (pendingMetrics != null && pendingMetrics.getValue() != null) {
                aggregated.merge(
                        MetricNames.PENDING_COMMITTABLES,
                        pendingMetrics.getValue().longValue(),
                        Long::sum);
            }
        }

        expected.entrySet()
                .forEach(e -> assertThat(aggregated, hasEntry(e.getKey(), e.getValue())));
    }

    private static class MetricWriter extends TestSinkV2.DefaultSinkWriter<Long> {
        static final long BASE_SEND_TIME = 100;
        static final long RECORD_SIZE_IN_BYTES = 10;
        private SinkWriterMetricGroup metricGroup;
        private long sendTime;

        @Override
        public void init(Sink.InitContext context) {
            this.metricGroup = context.metricGroup();
            metricGroup.setCurrentSendTimeGauge(() -> sendTime);
        }

        @Override
        public void write(Long element, Context context) {
            super.write(element, context);
            sendTime = element * BASE_SEND_TIME;
            metricGroup.getIOMetricGroup().getNumRecordsOutCounter().inc();
            if (element % 2 == 0) {
                metricGroup.getNumRecordsOutErrorsCounter().inc();
            }
            metricGroup.getIOMetricGroup().getNumBytesOutCounter().inc(RECORD_SIZE_IN_BYTES);
        }
    }

    private static class MetricCommitter extends TestSinkV2.DefaultCommitter {
        private int counter = 0;
        private SharedReference<CountDownLatch> beforeLatch;
        private SharedReference<CountDownLatch> afterLatch;

        MetricCommitter(
                SharedReference<CountDownLatch> beforeLatch,
                SharedReference<CountDownLatch> afterLatch) {
            this.beforeLatch = beforeLatch;
            this.afterLatch = afterLatch;
            this.counter = 0;
        }

        @Override
        public void commit(Collection<CommitRequest<String>> committables) {
            if (counter == 0) {
                System.err.println(
                        "Committables arrived "
                                + Thread.currentThread().getName()
                                + " "
                                + committables.stream()
                                        .map(c -> c.getCommittable())
                                        .collect(Collectors.toList()));
                committables.forEach(c -> c.retryLater());
            } else {
                if (counter == 1) {
                    // Wait for metrics check before continue
                    try {
                        committables.forEach(any -> beforeLatch.get().countDown());
                        afterLatch.get().await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

                committables.forEach(
                        c -> {
                            switch (c.getCommittable().charAt(1)) {
                                case '0':
                                    c.signalAlreadyCommitted();
                                    // 1 already committed
                                    break;
                                case '1':
                                case '2':
                                    // 2 failed
                                    c.signalFailedWithKnownReason(new RuntimeException());
                                    break;
                                case '3':
                                    // Retry without change
                                    if (counter == 1) {
                                        c.retryLater();
                                    }
                                    break;
                                case '4':
                                case '5':
                                    // Retry with change
                                    c.updateAndRetryLater("Retry-" + c.getCommittable());
                            }
                        });
            }
            counter++;
        }
    }
}
