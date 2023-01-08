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

package org.apache.flink.test.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart;
import static org.apache.flink.contrib.streaming.state.RocksDBMemoryControllerUtils.calculateActualCacheCapacity;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that {@link RocksDBOptions#FIX_PER_TM_MEMORY_SIZE} works as expected, i.e. make RocksDB use
 * the same BlockCache and WriteBufferManager objects. It does so using RocksDB metrics.
 */
public class TaskManagerWideRocksDbMemorySharingITCase {
    private static final int PARALLELISM = 4;
    private static final int NUMBER_OF_JOBS = 5;
    private static final int NUMBER_OF_TASKS = NUMBER_OF_JOBS * PARALLELISM;

    private static final MemorySize SHARED_MEMORY = MemorySize.ofMebiBytes(NUMBER_OF_TASKS * 25);
    private static final double WRITE_BUFFER_RATIO = 0.5;
    private static final double EXPECTED_BLOCK_CACHE_SIZE =
            calculateActualCacheCapacity(SHARED_MEMORY.getBytes(), WRITE_BUFFER_RATIO);
    // try to check that the memory usage is limited
    // however, there is no hard limit actually
    // because of https://issues.apache.org/jira/browse/FLINK-15532
    private static final double EFFECTIVE_LIMIT = EXPECTED_BLOCK_CACHE_SIZE * 1.5;

    private InMemoryReporter metricsReporter;
    private MiniClusterWithClientResource cluster;

    @Before
    public void init() throws Exception {
        metricsReporter = InMemoryReporter.create();
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(getConfiguration(metricsReporter))
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(NUMBER_OF_TASKS)
                                .build());
        cluster.before();
    }

    @After
    public void destroy() {
        cluster.after();
        metricsReporter.close();
    }

    @Test
    public void testBlockCache() throws Exception {
        List<JobID> jobIDs = new ArrayList<>(NUMBER_OF_JOBS);
        try {
            // launch jobs
            for (int i = 0; i < NUMBER_OF_JOBS; i++) {
                jobIDs.add(cluster.getRestClusterClient().submitJob(dag()).get());
            }

            // wait for init
            Deadline initDeadline = Deadline.fromNow(Duration.ofMinutes(1));
            for (JobID jid : jobIDs) {
                waitForAllTaskRunning(cluster.getMiniCluster(), jid, false);
                waitForAllMetricsReported(jid, initDeadline);
            }

            // check declared capacity
            collectGaugeValues(jobIDs, "rocksdb.block-cache-capacity")
                    .forEach(
                            size ->
                                    assertEquals(
                                            "Unexpected rocksdb block cache capacity",
                                            EXPECTED_BLOCK_CACHE_SIZE,
                                            size,
                                            0));

            // do some work and check the actual usage of memory
            for (int i = 0; i < 10; i++) {
                Thread.sleep(50L);
                DoubleSummaryStatistics stats =
                        collectGaugeValues(jobIDs, "rocksdb.block-cache-usage")
                                .collect(Collectors.summarizingDouble((Double::doubleValue)));
                assertEquals(
                        String.format(
                                "Block cache usage reported by different tasks varies too much: %s\n"
                                        + "That likely mean that they use different cache objects",
                                stats),
                        stats.getMax(),
                        stats.getMin(),
                        // some deviation is possible because:
                        // 1. records are being processed in parallel with requesting metrics
                        // 2. reporting metrics is not synchronized
                        500_000d);
                assertTrue(
                        String.format(
                                "total block cache usage is too high: %s (limit: %s, effective limit: %s)",
                                stats, EXPECTED_BLOCK_CACHE_SIZE, EFFECTIVE_LIMIT),
                        stats.getMax() <= EFFECTIVE_LIMIT);
            }

        } finally {
            for (JobID jobID : jobIDs) {
                cluster.getRestClusterClient().cancel(jobID).get();
            }
        }
    }

    private void waitForAllMetricsReported(JobID jid, Deadline deadline)
            throws InterruptedException {
        List<Double> gaugeValues = collectGaugeValues(jid, "rocksdb.block-cache-capacity");
        while (deadline.hasTimeLeft() && isEmptyOrHasZeroes(gaugeValues)) {
            Thread.sleep(100);
            gaugeValues = collectGaugeValues(jid, "rocksdb.block-cache-capacity");
        }
        if (isEmptyOrHasZeroes(gaugeValues)) {
            Assert.fail(
                    String.format(
                            "some tasks are still reporting zero cache capacity: %s", gaugeValues));
        }
    }

    private boolean isEmptyOrHasZeroes(List<Double> gaugeValues) {
        return gaugeValues.isEmpty() || gaugeValues.stream().anyMatch(x -> x == 0);
    }

    // collect at least one metric according to the given pattern
    // then convert it to double and return
    private List<Double> collectGaugeValues(JobID jobID, String metricPattern) {
        //noinspection unchecked
        List<Double> list =
                metricsReporter.findJobMetricGroups(jobID, metricPattern).stream()
                        .map(triple -> ((Gauge<BigInteger>) triple.f2).getValue().doubleValue())
                        .collect(Collectors.toList());
        checkState(!list.isEmpty());
        return list;
    }

    private Stream<Double> collectGaugeValues(List<JobID> jobIDs, String metricPattern) {
        return jobIDs.stream().flatMap(jobID -> collectGaugeValues(jobID, metricPattern).stream());
    }

    private JobGraph dag() {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(PARALLELISM);

        // don't flush memtables by checkpoints
        env.enableCheckpointing(24 * 60 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(noRestart());

        DataStreamSource<Long> src = env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE);
        src.keyBy(number -> number)
                .map(
                        new RichMapFunction<Long, Long>() {
                            private ListState<byte[]> state;
                            private int payloadSize;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                this.state =
                                        getRuntimeContext()
                                                .getListState(
                                                        new ListStateDescriptor<>(
                                                                "state", byte[].class));
                                // let each task to grow its state at a different speed
                                // to increase the probability of reporting different memory usages
                                // among different tasks
                                this.payloadSize = 4 + new Random().nextInt(7);
                            }

                            @Override
                            public Long map(Long value) throws Exception {
                                state.add(new byte[payloadSize]);
                                Thread.sleep(1L);
                                return value;
                            }
                        })
                .addSink(new DiscardingSink<>());
        return env.getStreamGraph().getJobGraph();
    }

    private static Configuration getConfiguration(InMemoryReporter metricsReporter) {
        Configuration configuration = new Configuration();

        configuration.set(RocksDBOptions.FIX_PER_TM_MEMORY_SIZE, SHARED_MEMORY);

        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        configuration.set(RocksDBOptions.USE_MANAGED_MEMORY, false);
        configuration.setDouble(RocksDBOptions.WRITE_BUFFER_RATIO, WRITE_BUFFER_RATIO);

        metricsReporter.addToConfiguration(configuration);
        configuration.set(RocksDBNativeMetricOptions.BLOCK_CACHE_CAPACITY, true);
        configuration.set(RocksDBNativeMetricOptions.BLOCK_CACHE_USAGE, true);

        return configuration;
    }
}
