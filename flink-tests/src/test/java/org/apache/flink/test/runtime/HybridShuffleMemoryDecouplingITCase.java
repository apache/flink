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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSource;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for hybrid shuffle with memory decoupling enabled. */
public class HybridShuffleMemoryDecouplingITCase extends BatchShuffleITCaseBase {

    private static final int NUM_RECORDS = 10000;
    private static final int NUM_RESULT_PARTITIONS = 10;
    private static final int PARALLELISM = 10;

    @Test
    void testHybridWithMultipleResultPartitionsCauseInsufficientBuffers() {
        Configuration configuration = configureHybridOptions(getConfiguration(), false);
        JobGraph jobGraph =
                createJobGraphWithMultipleResultPartitions(
                        NUM_RECORDS, false, NUM_RESULT_PARTITIONS, PARALLELISM, configuration);

        assertThatThrownBy(() -> JobGraphRunningUtil.execute(jobGraph, configuration, 1, 1))
                .hasStackTraceContaining("Insufficient number of network buffers");
    }

    @Test
    void testHybridWithMultipleResultPartitionsWithMemoryDecoupling() throws Exception {
        Configuration configuration = configureHybridOptions(getConfiguration(), true);
        JobGraph jobGraph =
                createJobGraphWithMultipleResultPartitions(
                        NUM_RECORDS, false, NUM_RESULT_PARTITIONS, PARALLELISM, configuration);

        JobGraphRunningUtil.execute(jobGraph, configuration, 1, 1);
        checkAllDataReceived(NUM_RECORDS, PARALLELISM, NUM_RESULT_PARTITIONS);
    }

    private JobGraph createJobGraphWithMultipleResultPartitions(
            int numRecordsToSend,
            boolean failExecution,
            int numResultPartitions,
            int parallelism,
            Configuration configuration) {
        configuration.setBoolean(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 0L));
        env.setParallelism(parallelism);

        DataStream<String> source =
                new DataStreamSource<>(
                        env,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new StreamSource<>(new StringSource(numRecordsToSend)),
                        true,
                        "source",
                        Boundedness.BOUNDED);

        for (int i = 0; i < numResultPartitions; i++) {
            source.map(value -> value).rebalance().addSink(new VerifySink(failExecution, false));
        }

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobType(JobType.BATCH);
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    private Configuration configureHybridOptions(
            Configuration configuration, boolean enableMemoryDecoupling) {
        configuration.set(
                ExecutionOptions.BATCH_SHUFFLE_MODE,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE);
        configuration.setBoolean(
                NettyShuffleEnvironmentOptions.NETWORK_HYBRID_SHUFFLE_ENABLE_MEMORY_DECOUPLING,
                enableMemoryDecoupling);
        configuration.setString(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "3200k");

        return configuration;
    }
}
