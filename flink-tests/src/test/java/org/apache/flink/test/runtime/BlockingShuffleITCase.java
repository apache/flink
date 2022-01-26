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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

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
        JobGraphRunningUtil.execute(
                jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
    }

    @Test
    public void testDeletePartitionFileOfSortMergeBlockingShuffle() throws Exception {
        Configuration configuration = getConfiguration();
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM, 1);

        JobGraph jobGraph = createJobGraph(0, true);
        JobGraphRunningUtil.execute(
                jobGraph, configuration, numTaskManagers, numSlotsPerTaskManager);
    }

    private Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.TMP_DIRS, TEMP_FOLDER.getRoot().getAbsolutePath());
        configuration.set(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 100);
        return configuration;
    }

    private JobGraph createJobGraph(int numRecordsToSend, boolean deletePartitionFile) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));
        env.setBufferTimeout(-1);
        env.setParallelism(numTaskManagers * numSlotsPerTaskManager);
        DataStream<String> source = env.addSource(new StringSource(numRecordsToSend));
        source.rebalance()
                .map((MapFunction<String, String>) value -> value)
                .broadcast()
                .addSink(new VerifySink(deletePartitionFile));

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
        // a scheduler supporting batch jobs is required for this job graph, because it contains
        // blocking data exchanges.
        // The scheduler is selected based on the JobType.
        streamGraph.setJobType(JobType.BATCH);
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    private static class StringSource implements ParallelSourceFunction<String> {
        private volatile boolean isRunning = true;
        private int numRecordsToSend;

        StringSource(int numRecordsToSend) {
            this.numRecordsToSend = numRecordsToSend;
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
    }

    private static class VerifySink extends RichSinkFunction<String> {
        private final boolean deletePartitionFile;

        VerifySink(boolean deletePartitionFile) {
            this.deletePartitionFile = deletePartitionFile;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (!deletePartitionFile || getRuntimeContext().getAttemptNumber() > 0) {
                return;
            }

            synchronized (BlockingShuffleITCase.class) {
                deleteFiles(TEMP_FOLDER.getRoot());
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            assertEquals(RECORD, value);
        }

        private static void deleteFiles(File root) throws IOException {
            File[] files = root.listFiles();
            if (files == null || files.length == 0) {
                return;
            }

            for (File file : files) {
                if (!file.isDirectory()) {
                    Files.deleteIfExists(file.toPath());
                } else {
                    deleteFiles(file);
                }
            }
        }
    }
}
