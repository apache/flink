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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.LongStream;

import static org.apache.flink.metrics.testutils.MetricMatchers.isCounter;
import static org.apache.flink.metrics.testutils.MetricMatchers.isGauge;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/** Tests whether all provided metrics of a {@link Sink} are of the expected values (FLIP-33). */
public class SinkMetricsITCase extends TestLogger {
    private static final int DEFAULT_PARALLELISM = 4;
    @Rule public final SharedObjects sharedObjects = SharedObjects.create();
    private InMemoryReporter reporter;

    private MiniClusterWithClientResource miniClusterResource;

    @Before
    public void setup() throws Exception {
        reporter = InMemoryReporter.createWithRetainedMetrics();
        Configuration configuration = new Configuration();
        reporter.addToConfiguration(configuration);
        miniClusterResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                .setConfiguration(configuration)
                                .build());
        miniClusterResource.before();
    }

    @After
    public void teardown() {
        miniClusterResource.after();
    }

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
                .sinkTo(TestSink.newBuilder().setWriter(new MetricWriter()).build())
                .name("MetricTestSink");
        JobClient jobClient = env.executeAsync();

        beforeBarrier.get().await();
        assertSinkMetrics(stopAtRecord1, env.getParallelism(), numSplits);
        afterBarrier.get().await();

        beforeBarrier.get().await();
        assertSinkMetrics(stopAtRecord2, env.getParallelism(), numSplits);
        afterBarrier.get().await();

        jobClient.getJobExecutionResult().get();
    }

    private void assertSinkMetrics(
            long processedRecordsPerSubtask, int parallelism, int numSplits) {
        List<OperatorMetricGroup> groups = reporter.findOperatorMetricGroups("MetricTestSink");
        assertThat(groups, hasSize(parallelism));

        int subtaskWithMetrics = 0;
        for (OperatorMetricGroup group : groups) {
            Map<String, Metric> metrics = reporter.getMetricsByGroup(group);
            // there are only 2 splits assigned; so two groups will not update metrics
            if (group.getIOMetricGroup().getNumRecordsOutCounter().getCount() == 0) {
                continue;
            }
            subtaskWithMetrics++;
            // I/O metrics
            assertThat(
                    group.getIOMetricGroup().getNumRecordsOutCounter(),
                    isCounter(equalTo(processedRecordsPerSubtask)));
            assertThat(
                    group.getIOMetricGroup().getNumBytesOutCounter(),
                    isCounter(
                            equalTo(
                                    processedRecordsPerSubtask
                                            * MetricWriter.RECORD_SIZE_IN_BYTES)));
            // MetricWriter is just incrementing errors every even record
            assertThat(
                    metrics.get(MetricNames.NUM_RECORDS_OUT_ERRORS),
                    isCounter(equalTo((processedRecordsPerSubtask + 1) / 2)));
            // check if the latest send time is fetched
            assertThat(
                    metrics.get(MetricNames.CURRENT_SEND_TIME),
                    isGauge(
                            equalTo(
                                    (processedRecordsPerSubtask - 1)
                                            * MetricWriter.BASE_SEND_TIME)));
        }
        assertThat(subtaskWithMetrics, equalTo(numSplits));
    }

    private static class MetricWriter extends TestSink.DefaultSinkWriter<Long> {
        static final long BASE_SEND_TIME = 100;
        static final long RECORD_SIZE_IN_BYTES = 10;
        private SinkWriterMetricGroup metricGroup;
        private long sendTime;
        private Counter recordsOutCounter;

        @Override
        public void init(Sink.InitContext context) {
            this.metricGroup = context.metricGroup();
            this.recordsOutCounter = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
            metricGroup.setCurrentSendTimeGauge(() -> sendTime);
        }

        @Override
        public void write(Long element, Context context) {
            super.write(element, context);
            sendTime = element * BASE_SEND_TIME;
            recordsOutCounter.inc();
            if (element % 2 == 0) {
                metricGroup.getNumRecordsOutErrorsCounter().inc();
            }
            metricGroup.getIOMetricGroup().getNumBytesOutCounter().inc(RECORD_SIZE_IN_BYTES);
        }
    }
}
