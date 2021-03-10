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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.junit.Assume.assumeTrue;

/** Tests for Reactive Mode (FLIP-159). */
public class ReactiveModeITCase extends TestLogger {
    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int INITIAL_NUMBER_TASK_MANAGERS = 1;

    private static final Configuration configuration = getReactiveModeConfiguration();

    @Rule
    public final MiniClusterResource miniClusterResource =
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

    @Before
    public void assumeDeclarativeResourceManagement() {
        assumeTrue(ClusterOptions.isDeclarativeResourceManagementEnabled(configuration));
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
                env.addSource(new ParallelismTrackingSource()).setMaxParallelism(1);
        input.addSink(new ParallelismTrackingSink<>()).getTransformation().setMaxParallelism(1);

        ParallelismTrackingSource.expectInstances(1);
        ParallelismTrackingSink.expectInstances(1);

        env.executeAsync();

        ParallelismTrackingSource.waitForInstances();
        ParallelismTrackingSink.waitForInstances();
    }

    /** Test that a job scales up when a TaskManager gets added to the cluster. */
    @Test
    public void testScaleUpOnAdditionalTaskManager() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> input = env.addSource(new ParallelismTrackingSource());
        input.addSink(new ParallelismTrackingSink<>());

        ParallelismTrackingSource.expectInstances(
                NUMBER_SLOTS_PER_TASK_MANAGER * INITIAL_NUMBER_TASK_MANAGERS);
        ParallelismTrackingSink.expectInstances(
                NUMBER_SLOTS_PER_TASK_MANAGER * INITIAL_NUMBER_TASK_MANAGERS);

        env.executeAsync();

        ParallelismTrackingSource.waitForInstances();
        ParallelismTrackingSink.waitForInstances();

        // expect scale up to 2 TaskManagers:
        ParallelismTrackingSource.expectInstances(NUMBER_SLOTS_PER_TASK_MANAGER * 2);
        ParallelismTrackingSink.expectInstances(NUMBER_SLOTS_PER_TASK_MANAGER * 2);

        miniClusterResource.getMiniCluster().startTaskManager();

        ParallelismTrackingSource.waitForInstances();
        ParallelismTrackingSink.waitForInstances();
    }

    @Test
    public void testScaleDownOnTaskManagerLoss() throws Exception {
        // test preparation: ensure we have 2 TaskManagers running
        startAdditionalTaskManager();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
        final DataStream<String> input = env.addSource(new ParallelismTrackingSource());
        input.addSink(new ParallelismTrackingSink<>());

        ParallelismTrackingSource.expectInstances(NUMBER_SLOTS_PER_TASK_MANAGER * 2);
        ParallelismTrackingSink.expectInstances(NUMBER_SLOTS_PER_TASK_MANAGER * 2);

        env.executeAsync();

        ParallelismTrackingSource.waitForInstances();
        ParallelismTrackingSink.waitForInstances();

        // scale down to 1 TaskManagers:
        ParallelismTrackingSource.expectInstances(NUMBER_SLOTS_PER_TASK_MANAGER);
        ParallelismTrackingSink.expectInstances(NUMBER_SLOTS_PER_TASK_MANAGER);

        miniClusterResource.getMiniCluster().terminateTaskManager(0).get();

        ParallelismTrackingSource.waitForInstances();
        ParallelismTrackingSink.waitForInstances();
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
        CommonTestUtils.waitUntilCondition(
                () -> getNumberOfConnectedTaskManagers() == 2,
                Deadline.fromNow(Duration.ofMillis(10_000L)));
    }

    private static class ParallelismTrackingSource implements ParallelSourceFunction<String> {
        private volatile boolean running = true;

        private static final InstanceTracker instances = new InstanceTracker();

        public static void expectInstances(int count) {
            instances.expectInstances(count);
        }

        public static void waitForInstances() throws InterruptedException {
            instances.waitForInstances();
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            instances.reportNewInstance();
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

    private static class ParallelismTrackingSink<T> extends RichSinkFunction<T> {

        private static final InstanceTracker instances = new InstanceTracker();

        public static void expectInstances(int count) {
            instances.expectInstances(count);
        }

        public static void waitForInstances() throws InterruptedException {
            instances.waitForInstances();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            instances.reportNewInstance();
        }
    }

    private static class InstanceTracker {
        private final Object lock = new Object();

        @GuardedBy("lock")
        private CountDownLatch latch = new CountDownLatch(0);

        public void reportNewInstance() {
            synchronized (lock) {
                if (latch.getCount() == 0) {
                    throw new RuntimeException("Test error. More instances than expected.");
                }
                latch.countDown();
            }
        }

        public void waitForInstances() throws InterruptedException {
            //noinspection FieldAccessNotGuarded
            latch.await();
        }

        public void expectInstances(int count) {
            synchronized (lock) {
                Preconditions.checkState(
                        latch.getCount() == 0, "Assuming previous latch has triggered");
                latch = new CountDownLatch(count);
            }
        }
    }
}
