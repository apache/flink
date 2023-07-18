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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SerializableSupplier;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

/**
 * Test that failure on recovery leads to job restart if configured, so that transient recovery
 * failures can are mitigated.
 */
public class CheckpointStoreITCase extends TestLogger {

    private static final Configuration CONFIGURATION =
            new Configuration()
                    .set(
                            HighAvailabilityOptions.HA_MODE,
                            BlockingHighAvailabilityServiceFactory.class.getName());

    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(CONFIGURATION)
                            .build());

    @Before
    public void setUp() {
        BlockingHighAvailabilityServiceFactory.reset();
        FailingMapper.reset();
    }

    @Test
    public void testJobClientRemainsResponsiveDuringCompletedCheckpointStoreRecovery()
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);
        env.setRestartStrategy(fixedDelayRestart(2 /* failure on processing + on recovery */, 0));
        env.addSource(emitUntil(() -> FailingMapper.failedAndProcessed))
                .map(new FailingMapper())
                .sinkTo(new DiscardingSink<>());
        final JobClient jobClient = env.executeAsync();

        BlockingHighAvailabilityServiceFactory.fetchRemoteCheckpointsStart.await();
        for (int i = 0; i < 10; i++) {
            final JobStatus jobStatus = jobClient.getJobStatus().get();
            assertEquals(JobStatus.INITIALIZING, jobStatus);
        }
        BlockingHighAvailabilityServiceFactory.fetchRemoteCheckpointsFinished.countDown();

        // Await for job to finish.
        jobClient.getJobExecutionResult().get();

        checkState(FailingMapper.failedAndProcessed);
    }

    private static class FailingMapper implements MapFunction<Integer, Integer> {

        private static volatile boolean failed = false;
        private static volatile boolean failedAndProcessed = false;

        static void reset() {
            failed = false;
            failedAndProcessed = false;
        }

        @Override
        public Integer map(Integer element) throws Exception {
            if (!failed) {
                failed = true;
                throw new RuntimeException();
            } else {
                failedAndProcessed = true;
                return element;
            }
        }
    }

    /**
     * Testing implementation of {@link HighAvailabilityServicesFactory} that lets us inject custom
     * {@link HighAvailabilityServices}.
     */
    public static class BlockingHighAvailabilityServiceFactory
            implements HighAvailabilityServicesFactory {

        private static volatile CountDownLatch fetchRemoteCheckpointsStart = new CountDownLatch(1);
        private static volatile CountDownLatch fetchRemoteCheckpointsFinished =
                new CountDownLatch(1);

        static void reset() {
            fetchRemoteCheckpointsStart = new CountDownLatch(1);
            fetchRemoteCheckpointsFinished = new CountDownLatch(1);
        }

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            final CheckpointRecoveryFactory checkpointRecoveryFactory =
                    PerJobCheckpointRecoveryFactory.withoutCheckpointStoreRecovery(
                            maxCheckpoints -> {
                                fetchRemoteCheckpointsStart.countDown();
                                try {
                                    fetchRemoteCheckpointsFinished.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return new StandaloneCompletedCheckpointStore(maxCheckpoints);
                            });
            return new EmbeddedHaServicesWithLeadershipControl(executor, checkpointRecoveryFactory);
        }
    }

    private SourceFunction<Integer> emitUntil(SerializableSupplier<Boolean> until) {
        return new SourceFunction<Integer>() {

            private volatile boolean running = true;

            @Override
            public void run(SourceContext<Integer> ctx) {
                while (running && !until.get()) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(0);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            ExceptionUtils.rethrow(e);
                        }
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        };
    }
}
