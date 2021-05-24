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
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SerializableSupplier;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.apache.flink.configuration.JobManagerOptions.SchedulerType.Adaptive;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assume.assumeFalse;

/**
 * Test that failure on recovery leads to job restart if configured, so that transient recovery
 * failures can are mitigated.
 */
public class CheckpointStoreITCase extends TestLogger {

    private static final Configuration CONFIGURATION =
            new Configuration()
                    .set(HighAvailabilityOptions.HA_MODE, TestingHAFactory.class.getName());

    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(CONFIGURATION)
                            .build());

    @Before
    public void init() {
        FailingStore.reset();
        FailingMapper.reset();
    }

    @Test
    public void testRestartOnRecoveryFailure() throws Exception {
        assumeFalse(
                // TODO: remove after FLINK-22483
                "Adaptive scheduler doesn't retry after failures on recovery",
                ClusterOptions.getSchedulerType(CONFIGURATION) == Adaptive);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);
        env.setRestartStrategy(fixedDelayRestart(2 /* failure on processing + on recovery */, 0));
        env.addSource(emitUntil(() -> FailingStore.recovered && FailingMapper.failedAndProcessed))
                .map(new FailingMapper())
                .addSink(new DiscardingSink<>());
        env.execute();

        checkState(FailingStore.recovered && FailingMapper.failedAndProcessed);
    }

    private static class FailingMapper implements MapFunction<Integer, Integer> {
        private static volatile boolean failed = false;
        private static volatile boolean failedAndProcessed = false;

        public static void reset() {
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

    /** TestingHAFactory. */
    public static class TestingHAFactory implements HighAvailabilityServicesFactory {

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            return new EmbeddedHaServices(Executors.directExecutor()) {

                @Override
                public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
                    return new TestingCheckpointRecoveryFactory(
                            new FailingStore(),
                            new TestingCheckpointIDCounter(new CompletableFuture<>()));
                }
            };
        }
    }

    private static class FailingStore implements CompletedCheckpointStore {
        private static volatile boolean started = false;
        private static volatile boolean failed = false;
        private static volatile boolean recovered = false;

        public static void reset() {
            started = failed = recovered = false;
        }

        @Override
        public void recover() throws Exception {
            if (!started) {
                started = true;
            } else if (!failed) {
                failed = true;
                throw new RuntimeException();
            } else if (!recovered) {
                recovered = true;
            }
        }

        @Override
        public void addCheckpoint(
                CompletedCheckpoint checkpoint,
                CheckpointsCleaner checkpointsCleaner,
                Runnable postCleanup) {}

        @Override
        public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
                throws Exception {}

        @Override
        public List<CompletedCheckpoint> getAllCheckpoints() {
            return Collections.emptyList();
        }

        @Override
        public int getNumberOfRetainedCheckpoints() {
            return 0;
        }

        @Override
        public int getMaxNumberOfRetainedCheckpoints() {
            return 1;
        }

        @Override
        public boolean requiresExternalizedCheckpoints() {
            return false;
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
