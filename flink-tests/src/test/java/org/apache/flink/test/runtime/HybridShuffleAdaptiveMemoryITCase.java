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

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

/**
 * Tests an immediate checkpoint should be triggered right after all tasks reached the end of data.
 */
public class HybridShuffleAdaptiveMemoryITCase extends AbstractTestBase {
    private static final int SMALL_SOURCE_NUM_RECORDS = 20;
    private static final int BIG_SOURCE_NUM_RECORDS = 100;

    private StreamExecutionEnvironment env;
    private SharedReference<List<Integer>> smallResult;
    private SharedReference<List<Integer>> bigResult;

    @TempDir private java.nio.file.Path tmpDir;

    //    @RegisterExtension
    //    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    @BeforeEach
    public void setUp() {
        Configuration configuration = getConfiguration(true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(4);

        //        Configuration configuration = getConfiguration(false);
        //        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        //        env.setParallelism(4);
        //        smallResult = sharedObjects.add(new CopyOnWriteArrayList<>());
        //        bigResult = sharedObjects.add(new CopyOnWriteArrayList<>());
        //        IntegerStreamSource.failedBefore = false;
        //        IntegerStreamSource.latch = new CountDownLatch(1);
    }

    private Configuration getConfiguration(boolean isSelective) {
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        configuration.set(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX, 100);
        configuration.setBoolean(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);

        BatchShuffleMode shuffleMode =
                isSelective
                        ? BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE
                        : BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL;
        configuration.set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode);
        configuration.set(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE, true);

        return configuration;
    }

    @Test
    public void testResultPartition() throws Exception {

        final Configuration config = getConfiguration(true);
        config.setString(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "1536k");

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .withRandomPorts()
                        .setNumTaskManagers(2)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            DataStream<String> source =
                    new DataStreamSource<>(
                            env,
                            BasicTypeInfo.STRING_TYPE_INFO,
                            new StreamSource<>(new StringSource(1000)),
                            true,
                            "source",
                            Boundedness.BOUNDED);

            source.map(x -> x.concat("x")).rebalance().addSink(new PrintSinkFunction<>());
            source.map(x -> x.concat("y")).rebalance().addSink(new PrintSinkFunction<>());
            source.map(x -> x.concat("z")).rebalance().addSink(new PrintSinkFunction<>());
            //            source.shuffle().addSink(new PrintSinkFunction<>());
            //            source.keyBy(x -> x).reduce((a, b) -> a.concat(b)).addSink(new
            // PrintSinkFunction<>());

            StreamGraph streamGraph = env.getStreamGraph();
            streamGraph.setJobType(JobType.BATCH);
            JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    @Test
    public void testInputGate() throws Exception {

        final Configuration config = getConfiguration(true);
        config.setString(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "1024k");
        config.setInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL, 8);

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .withRandomPorts()
                        .setNumTaskManagers(2)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            DataStream<String> source =
                    new DataStreamSource<>(
                            env,
                            BasicTypeInfo.STRING_TYPE_INFO,
                            new StreamSource<>(new StringSource(1000)),
                            true,
                            "source",
                            Boundedness.BOUNDED);

            source.rebalance().addSink(new PrintSinkFunction<>());
            //            source.map(x -> x.concat("y")).rebalance().addSink(new
            // PrintSinkFunction<>());

            StreamGraph streamGraph = env.getStreamGraph();
            streamGraph.setJobType(JobType.BATCH);
            JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

            miniCluster.executeJobBlocking(jobGraph);
        }
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
                ctx.collect("abc");
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
