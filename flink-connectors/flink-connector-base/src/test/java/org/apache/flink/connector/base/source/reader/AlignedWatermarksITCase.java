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
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An IT test to verify watermark alignment works. The idea is that we check the {@link
 * MetricNames#WATERMARK_ALIGNMENT_DRIFT} metric does not grow beyond the maximal configured drift.
 * It can initially increase beyond that value, because of the update interval, but once established
 * it should never increase any further, but gradually decrease to the configured threshold, as the
 * slower source catches up.
 */
public class AlignedWatermarksITCase {
    public static final String SLOW_SOURCE_NAME = "SlowNumberSequenceSource";
    public static final String FAST_SOURCE_NAME = "FastNumberSequenceSource";
    private static final Duration UPDATE_INTERVAL = Duration.ofMillis(100);
    public static final int MAX_DRIFT = 10;

    @RegisterExtension
    LoggerAuditingExtension loggerAuditingExtension =
            new LoggerAuditingExtension(AlignedWatermarksITCase.class, Level.INFO);

    private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setConfiguration(reporter.addToConfiguration(new Configuration()))
                            .build());

    @Test
    public void testAlignment(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        final JobGraph jobGraph = getJobGraph();
        final CompletableFuture<JobSubmissionResult> submission = miniCluster.submitJob(jobGraph);
        final JobID jobID = submission.get().getJobID();
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobID, false);

        long oldDrift = Long.MAX_VALUE;
        do {
            final Optional<Metric> drift =
                    reporter.findMetric(
                            jobID, FAST_SOURCE_NAME + ".*" + MetricNames.WATERMARK_ALIGNMENT_DRIFT);
            Thread.sleep(200);

            final Optional<Long> newDriftOptional = drift.map(m -> ((Gauge<Long>) m).getValue());
            if (newDriftOptional.isPresent()) {
                final Long newDrift = newDriftOptional.get();
                assertThat(newDrift).isLessThanOrEqualTo(oldDrift);
                oldDrift = newDrift;
            }
        } while (oldDrift >= MAX_DRIFT);
    }

    private JobGraph getJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(0L);
        env.setParallelism(1);

        DataStream<Long> slowSource =
                env.fromSource(
                                new NumberSequenceSource(0, Long.MAX_VALUE),
                                WatermarkStrategy.forGenerator(ctx -> new PunctuatedGenerator())
                                        .withWatermarkAlignment(
                                                "group-1",
                                                Duration.ofMillis(MAX_DRIFT),
                                                UPDATE_INTERVAL)
                                        .withTimestampAssigner((r, t) -> r),
                                SLOW_SOURCE_NAME)
                        .map(
                                new RichMapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        Thread.sleep(10);
                                        return value;
                                    }
                                });

        DataStream<Long> fastSource =
                env.fromSource(
                                new NumberSequenceSource(0, Long.MAX_VALUE),
                                WatermarkStrategy.forGenerator(ctx -> new PunctuatedGenerator())
                                        .withWatermarkAlignment(
                                                "group-1",
                                                Duration.ofMillis(MAX_DRIFT),
                                                UPDATE_INTERVAL)
                                        .withTimestampAssigner((r, t) -> r),
                                FAST_SOURCE_NAME)
                        .map(
                                new RichMapFunction<Long, Long>() {
                                    @Override
                                    public Long map(Long value) throws Exception {
                                        Thread.sleep(1);
                                        return value;
                                    }
                                });

        slowSource.union(fastSource).sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static class PunctuatedGenerator implements WatermarkGenerator<Long> {
        @Override
        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }
}
