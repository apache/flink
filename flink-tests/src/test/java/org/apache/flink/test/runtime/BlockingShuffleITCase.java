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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.SerializedThrowable;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for blocking shuffle. */
public class BlockingShuffleITCase {

    private static final String RECORD = "hello, world!";

    private final int numTaskManagers = 2;

    private final int numSlotsPerTaskManager = 4;

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Test
    public void testBoundedBlockingShuffle() throws Exception {
        JobGraph jobGraph = createJobGraph(1000000, false);
        Configuration configuration = getConfiguration();
        JobGraphRunningUtil.execute(
                jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
    }

    @Test
    public void testBoundedBlockingShuffleWithoutData() throws Exception {
        JobGraph jobGraph = createJobGraph(0, false);
        Configuration configuration = getConfiguration();
        JobGraphRunningUtil.execute(
                jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
    }

    @Test
    public void testSortMergeBlockingShuffle() throws Exception {
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM, 1);

        JobGraph jobGraph = createJobGraph(1000000, false);
        JobGraphRunningUtil.execute(
                jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
    }

    @Test
    public void testSortMergeBlockingShuffleWithoutData() throws Exception {
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM, 1);

        JobGraph jobGraph = createJobGraph(0, false);
        JobGraphRunningUtil.execute(
                jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
    }

    @Test
    public void testDeletePartitionFileOfBoundedBlockingShuffle() throws Exception {
        Configuration configuration = getConfiguration();
        JobGraph jobGraph = createJobGraph(0, true);
        SerializedThrowable exception =
                JobGraphRunningUtil.executeAndGetThrowable(
                        jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
        assertNotNull(exception);
        assertTrue(exception.getFullStringifiedStackTrace().contains("PartitionNotFoundException"));
    }

    @Test
    public void testDeletePartitionFileOfSortMergeBlockingShuffle() throws Exception {
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM, 1);

        JobGraph jobGraph = createJobGraph(0, true);
        SerializedThrowable exception =
                JobGraphRunningUtil.executeAndGetThrowable(
                        jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
        assertNotNull(exception);
        assertTrue(exception.getFullStringifiedStackTrace().contains("PartitionNotFoundException"));
    }

    private Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.TMP_DIRS, TEMP_FOLDER.getRoot().getAbsolutePath());
        configuration.set(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 100);
        return configuration;
    }

    private JobGraph createJobGraph(int numRecordsToSend, boolean deletePartitionFile) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(numTaskManagers * numSlotsPerTaskManager);
        DataStream<String> source =
                env.addSource(new StringSource(numRecordsToSend, deletePartitionFile));
        source.rebalance()
                .map((MapFunction<String, String>) value -> value)
                .broadcast()
                .addSink(new VerifySink());

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalDataExchangeMode(GlobalDataExchangeMode.ALL_EDGES_BLOCKING);
        // a scheduler supporting batch jobs is required for this job graph, because it contains
        // blocking data exchanges.
        // The scheduler is selected based on the JobType.
        streamGraph.setJobType(JobType.BATCH);
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    private static class StringSource extends RichParallelSourceFunction<String> {

        private final boolean deletePartitionFile;
        private volatile boolean isRunning = true;
        private int numRecordsToSend;

        StringSource(int numRecordsToSend, boolean deletePartitionFile) {
            this.numRecordsToSend = numRecordsToSend;
            this.deletePartitionFile = deletePartitionFile;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning && numRecordsToSend-- > 0) {
                ctx.collect(RECORD);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void close() throws Exception {
            super.close();

            if (deletePartitionFile) {
                for (File directory : checkNotNull(TEMP_FOLDER.getRoot().listFiles())) {
                    if (directory.isDirectory()) {
                        for (File file : checkNotNull(directory.listFiles())) {
                            Files.deleteIfExists(file.toPath());
                        }
                    }
                }
            }
        }
    }

    private static class VerifySink implements SinkFunction<String> {

        @Override
        public void invoke(String value) throws Exception {
            assertEquals(RECORD, value);
        }
    }
}
