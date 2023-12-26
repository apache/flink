/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Reactive Mode (FLIP-159). */
public class ReactiveModeITCase extends TestLogger {
    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int INITIAL_NUMBER_TASK_MANAGERS = 1;

    private static final Configuration configuration = getReactiveModeConfiguration();

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberTaskManagers(INITIAL_NUMBER_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .build());

    private static Configuration getReactiveModeConfiguration() {
        final Configuration conf = new Configuration();
        conf.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
        return conf;
    }

    /**
     * Users can set maxParallelism and reactive mode must not run with a parallelism higher than
     * maxParallelism.
     */
    @Test
    public void testScaleLimitByMaxParallelism() throws Exception {
        // test preparation: ensure we have 2 TaskManagers running
        startAdditionalTaskManager();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // we set maxParallelism = 1 and assert it never exceeds it
        final DataStream<String> input =
                env.addSource(new FailOnParallelExecutionSource()).setMaxParallelism(1);
        input.sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();

        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(), jobClient.getJobID(), 1);
    }

    /** Test that a job scales up when a TaskManager gets added to the cluster. */
    @Test
    public void testScaleUpOnAdditionalTaskManager() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> input = env.addSource(new DummySource());
        input.sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();

        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(),
                jobClient.getJobID(),
                NUMBER_SLOTS_PER_TASK_MANAGER * INITIAL_NUMBER_TASK_MANAGERS);

        // scale up to 2 TaskManagers:
        miniClusterResource.getMiniCluster().startTaskManager();

        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(),
                jobClient.getJobID(),
                NUMBER_SLOTS_PER_TASK_MANAGER * (INITIAL_NUMBER_TASK_MANAGERS + 1));
    }

    @Test
    public void testJsonPlanParallelismAfterRescale() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> input = env.addSource(new DummySource());
        input.sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();

        int initialParallelism = NUMBER_SLOTS_PER_TASK_MANAGER * INITIAL_NUMBER_TASK_MANAGERS;
        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(),
                jobClient.getJobID(),
                initialParallelism);

        ArchivedExecutionGraph archivedExecutionGraph =
                miniClusterResource
                        .getMiniCluster()
                        .getArchivedExecutionGraph(jobClient.getJobID())
                        .get();

        assertThat(
                        OBJECT_MAPPER
                                .readTree(archivedExecutionGraph.getJsonPlan())
                                .findValues("parallelism"))
                .allMatch(n -> n.asInt() == initialParallelism);

        // scale up to 2 TaskManagers:
        miniClusterResource.getMiniCluster().startTaskManager();

        int rescaledParallelism =
                NUMBER_SLOTS_PER_TASK_MANAGER * (INITIAL_NUMBER_TASK_MANAGERS + 1);
        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(),
                jobClient.getJobID(),
                rescaledParallelism);

        archivedExecutionGraph =
                miniClusterResource
                        .getMiniCluster()
                        .getArchivedExecutionGraph(jobClient.getJobID())
                        .get();

        assertThat(
                        OBJECT_MAPPER
                                .readTree(archivedExecutionGraph.getJsonPlan())
                                .findValues("parallelism"))
                .allMatch(n -> n.asInt() == rescaledParallelism);
    }

    @Test
    public void testScaleDownOnTaskManagerLoss() throws Exception {
        // test preparation: ensure we have 2 TaskManagers running
        startAdditionalTaskManager();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure exactly one restart to avoid restart loops in error cases
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));
        final DataStream<String> input = env.addSource(new DummySource());
        input.sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();

        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(),
                jobClient.getJobID(),
                NUMBER_SLOTS_PER_TASK_MANAGER * (INITIAL_NUMBER_TASK_MANAGERS + 1));

        // scale down to 1 TaskManagers:
        miniClusterResource.getMiniCluster().terminateTaskManager(0).get();

        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(),
                jobClient.getJobID(),
                NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_SLOTS_PER_TASK_MANAGER);
    }

    /** Test for FLINK-28274. */
    @Test
    public void testContinuousFileMonitoringFunctionWithReactiveMode() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> input = env.readTextFile(tempFolder.getRoot().getPath());
        input.sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();

        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(), jobClient.getJobID(), 1);

        // scale up to 2 TaskManagers:
        miniClusterResource.getMiniCluster().startTaskManager();

        waitUntilParallelismForVertexReached(
                miniClusterResource.getRestClusterClient(), jobClient.getJobID(), 1);
    }

    private int getNumberOfConnectedTaskManagers() throws ExecutionException, InterruptedException {
        return miniClusterResource
                .getMiniCluster()
                .requestClusterOverview()
                .get()
                .getNumTaskManagersConnected();
    }

    private void startAdditionalTaskManager() throws Exception {
        miniClusterResource.getMiniCluster().startTaskManager();
        CommonTestUtils.waitUntilCondition(() -> getNumberOfConnectedTaskManagers() == 2);
    }

    private static class DummySource implements SourceFunction<String> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect("test");
                }
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class FailOnParallelExecutionSource extends RichParallelSourceFunction<String> {
        private volatile boolean running = true;

        @Override
        public void open(OpenContext openContext) throws Exception {
            if (getRuntimeContext().getNumberOfParallelSubtasks() > 1) {
                throw new IllegalStateException(
                        "This is not supposed to be executed in parallel, despite extending the right base class.");
            }
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect("test");
                }
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static void waitUntilParallelismForVertexReached(
            RestClusterClient<?> restClusterClient, JobID jobId, int targetParallelism)
            throws Exception {

        CommonTestUtils.waitUntilCondition(
                () -> {
                    JobDetailsInfo detailsInfo = restClusterClient.getJobDetails(jobId).get();

                    for (JobDetailsInfo.JobVertexDetailsInfo jobVertexInfo :
                            detailsInfo.getJobVertexInfos()) {
                        if (jobVertexInfo.getName().contains("Source:")
                                && jobVertexInfo.getParallelism() == targetParallelism) {
                            return true;
                        }
                    }
                    return false;
                });
    }
}
