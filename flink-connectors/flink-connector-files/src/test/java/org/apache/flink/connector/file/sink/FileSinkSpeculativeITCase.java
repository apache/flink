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

package org.apache.flink.connector.file.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SlowTaskDetectorOptions;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils;
import org.apache.flink.connector.file.sink.utils.PartSizeAndCheckpointRollingPolicy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link FileSink} with speculative scheduling enabled. */
class FileSinkSpeculativeITCase {

    @RegisterExtension
    private static final MiniClusterExtension miniClusterExtension =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .setConfiguration(configure())
                            .build());

    @TempDir private Path tmpDir;

    private static final int NUM_SOURCES = 4;

    private static final int NUM_SINKS = 3;

    private static final int NUM_RECORDS = 10000;

    private static final int NUM_BUCKETS = 4;

    private static final AtomicInteger slowTaskCounter = new AtomicInteger(1);

    @BeforeEach
    void setUp() {
        slowTaskCounter.set(1);
    }

    @Test
    void testFileSinkSpeculative() throws Exception {
        String path = tmpDir.toString();
        executeJobWithSlowSink(path);
        IntegerFileSinkTestDataUtils.checkIntegerSequenceSinkOutput(
                path, NUM_RECORDS, NUM_BUCKETS, NUM_SOURCES);
    }

    private void executeJobWithSlowSink(String path) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Create a testing job with a bounded source.
        StreamSource<Integer, ?> sourceOperator =
                new StreamSource<>(new BatchExecutionTestSource(NUM_RECORDS));
        DataStreamSource<Integer> source =
                new DataStreamSource<>(
                        env,
                        BasicTypeInfo.INT_TYPE_INFO,
                        sourceOperator,
                        true,
                        "Source",
                        Boundedness.BOUNDED);

        DataStreamSink<Integer> sink =
                source.setParallelism(NUM_SOURCES)
                        .rebalance()
                        .map(new TestingMap())
                        .name("test_map")
                        .setParallelism(NUM_SINKS)
                        .sinkTo(createFileSink(path))
                        .name("file_sink")
                        .setParallelism(NUM_SINKS);

        JobGraph jobGraph = env.getStreamGraph(false).getJobGraph();

        // Assert that the TestingMap operator is chained with FileSink, which will lead
        // to a slow sink as well.
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            if (jobVertex.getName().contains("test_map")) {
                assertThat(jobVertex.getName().contains("file_sink")).isTrue();
            }
        }

        JobClient client = env.executeAsync("FileSinkSpeculativeITCase");
        client.getJobExecutionResult().get();
    }

    private FileSink<Integer> createFileSink(String path) {
        return FileSink.forRowFormat(
                        new org.apache.flink.core.fs.Path(path),
                        new IntegerFileSinkTestDataUtils.IntEncoder())
                .withBucketAssigner(
                        new IntegerFileSinkTestDataUtils.ModuloBucketAssigner(NUM_BUCKETS))
                .withRollingPolicy(new PartSizeAndCheckpointRollingPolicy<>(1024, false))
                .build();
    }

    private static Configuration configure() {
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "0");
        configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 5000L);

        // for speculative execution
        configuration.set(BatchExecutionOptions.SPECULATIVE_ENABLED, true);

        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER, 1.0);
        configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO, 0.2);
        configuration.set(
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND, Duration.ofMillis(0));
        configuration.set(BatchExecutionOptions.BLOCK_SLOW_NODE_DURATION, Duration.ofMillis(0));

        return configuration;
    }

    /** A bounded batch source for testing. */
    private static class BatchExecutionTestSource extends RichParallelSourceFunction<Integer> {

        private final int numberOfRecords;

        private volatile boolean isCanceled;

        public BatchExecutionTestSource(int numberOfRecords) {
            this.numberOfRecords = numberOfRecords;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            for (int i = 0; !isCanceled && i < numberOfRecords; ++i) {
                ctx.collect(i);
            }
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }
    }

    /**
     * A testing map function that simulates the slow task. The TestingMap is chained with the
     * FileSink. Thus, we can simulate the slow task node for fileSink by testingMap.
     */
    private static class TestingMap extends RichMapFunction<Integer, Integer> {

        @Override
        public Integer map(Integer value) throws Exception {

            maybeSleep();

            return value;
        }
    }

    private static void maybeSleep() {
        if (slowTaskCounter.getAndDecrement() > 0) {
            try {
                Thread.sleep(Integer.MAX_VALUE);
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }
    }
}
