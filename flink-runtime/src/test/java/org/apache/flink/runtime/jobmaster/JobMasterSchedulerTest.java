/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.scheduler.TestingSchedulerNG;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the JobMaster scheduler interaction. */
public class JobMasterSchedulerTest extends TestLogger {

    @ClassRule
    public static final TestingRpcServiceResource TESTING_RPC_SERVICE_RESOURCE =
            new TestingRpcServiceResource();

    /** Tests that the JobMaster fails if we cannot start the scheduling. See FLINK-20382. */
    @Test
    public void testIfStartSchedulingFailsJobMasterFails() throws Exception {
        final SchedulerNGFactory schedulerFactory = new FailingSchedulerFactory();
        final JobMasterBuilder.TestingOnCompletionActions onCompletionActions =
                new JobMasterBuilder.TestingOnCompletionActions();
        final JobMaster jobMaster =
                new JobMasterBuilder(
                                new JobGraph(), TESTING_RPC_SERVICE_RESOURCE.getTestingRpcService())
                        .withSchedulerFactory(schedulerFactory)
                        .withOnCompletionActions(onCompletionActions)
                        .createJobMaster();

        jobMaster.start();

        assertThat(
                onCompletionActions.getJobMasterFailedFuture().join(),
                is(instanceOf(JobMasterException.class)));

        // close the jobMaster to remove it from the testing rpc service so that it can shut down
        // cleanly
        try {
            jobMaster.close();
        } catch (Exception expected) {
            // expected
        }
    }

    private static final class FailingSchedulerFactory implements SchedulerNGFactory {
        @Override
        public SchedulerNG createInstance(
                Logger log,
                JobGraph jobGraph,
                BackPressureStatsTracker backPressureStatsTracker,
                Executor ioExecutor,
                Configuration jobMasterConfiguration,
                SlotPool slotPool,
                ScheduledExecutorService futureExecutor,
                ClassLoader userCodeLoader,
                CheckpointRecoveryFactory checkpointRecoveryFactory,
                Time rpcTimeout,
                BlobWriter blobWriter,
                JobManagerJobMetricGroup jobManagerJobMetricGroup,
                Time slotRequestTimeout,
                ShuffleMaster<?> shuffleMaster,
                JobMasterPartitionTracker partitionTracker,
                ExecutionDeploymentTracker executionDeploymentTracker,
                long initializationTimestamp) {
            return TestingSchedulerNG.newBuilder()
                    .setStartSchedulingRunnable(
                            () -> {
                                throw new FlinkRuntimeException("Could not start scheduling.");
                            })
                    .build();
        }
    }
}
