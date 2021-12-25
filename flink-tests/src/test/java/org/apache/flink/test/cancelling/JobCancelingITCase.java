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

package org.apache.flink.test.cancelling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;

import static junit.framework.TestCase.assertEquals;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;

/** Tests for canceling the job. */
@SuppressWarnings("serial")
public class JobCancelingITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    @Test
    public void testCancelingWhileBackPressured() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().enableObjectReuse();
        // Basically disable interrupts and JVM killer watchdogs
        env.getConfig().setTaskCancellationTimeout(Duration.ofDays(1).toMillis());
        env.getConfig().setTaskCancellationInterval(Duration.ofDays(1).toMillis());

        // Check both FLIP-27 and normal sources
        final DataStreamSource<Long> source1 =
                env.fromSource(
                        new NumberSequenceSource(1L, Long.MAX_VALUE),
                        WatermarkStrategy.noWatermarks(),
                        "source-1");

        // otherwise split enumerator will generate splits that can start emitting from very large
        // numbers, that do not work well with ExplodingFlatMapFunction
        source1.setParallelism(1);
        final DataStream<Long> source2 = env.addSource(new InfiniteLongSourceFunction());

        source1.connect(source2)
                // Multiply number of records in flatMap function to try block task while
                // processing elements
                .flatMap(new ExplodingFlatMapFunction())
                // SleepingSink should be in another task, otherwise exploding flatMap above would
                // be blocked inside SleepingSink instead of the network stack
                .startNewChain()
                .addSink(new SleepingSink());

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();

        ClusterClient<?> client = MINI_CLUSTER.getClusterClient();
        JobID jobID = client.submitJob(jobGraph).get();
        waitForAllTaskRunning(MINI_CLUSTER.getMiniCluster(), jobID, false);
        // give a bit of time of back pressure to build up
        Thread.sleep(100);

        client.cancel(jobID).get();
        while (!client.getJobStatus(jobID).get().isTerminalState()) {}

        assertEquals(JobStatus.CANCELED, client.getJobStatus(jobID).get());
    }

    private static class InfiniteLongSourceFunction implements SourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long next = 0;
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(next++);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class ExplodingFlatMapFunction implements CoFlatMapFunction<Long, Long, Long> {
        @Override
        public void flatMap1(Long value, Collector<Long> out) throws Exception {
            emit(value, out);
        }

        @Override
        public void flatMap2(Long value, Collector<Long> out) throws Exception {
            emit(value, out);
        }

        private void emit(long value, Collector<Long> out) {
            for (long i = 0; i <= value; i++) {
                out.collect(i);
            }
        }
    }

    private static class SleepingSink implements SinkFunction<Long> {
        @Override
        public void invoke(Long value, Context context) throws Exception {
            Thread.sleep(1_000);
        }
    }
}
