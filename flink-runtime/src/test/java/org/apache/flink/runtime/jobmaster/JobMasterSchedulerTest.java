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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolServiceBuilder;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.scheduler.TestingSchedulerNG;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

class JobMasterSchedulerTest {

    private static final TestingRpcServiceExtension TESTING_RPC_SERVICE_EXTENSION =
            new TestingRpcServiceExtension();

    @RegisterExtension
    private static final AllCallbackWrapper<TestingRpcServiceExtension>
            RPC_SERVICE_EXTENSION_WRAPPER = new AllCallbackWrapper<>(TESTING_RPC_SERVICE_EXTENSION);

    /** Tests that the JobMaster fails if we cannot start the scheduling. See FLINK-20382. */
    @Test
    void testIfStartSchedulingFailsJobMasterFails() throws Exception {
        final SchedulerNGFactory schedulerFactory = new FailingSchedulerFactory();
        final JobMasterBuilder.TestingOnCompletionActions onCompletionActions =
                new JobMasterBuilder.TestingOnCompletionActions();
        final JobManagerSharedServices jobManagerSharedServices =
                new TestingJobManagerSharedServicesBuilder().build();
        try {
            final JobMaster jobMaster =
                    new JobMasterBuilder(
                                    JobGraphTestUtils.emptyJobGraph(),
                                    TESTING_RPC_SERVICE_EXTENSION.getTestingRpcService())
                            .withSlotPoolServiceSchedulerFactory(
                                    DefaultSlotPoolServiceSchedulerFactory.create(
                                            TestingSlotPoolServiceBuilder.newBuilder(),
                                            schedulerFactory))
                            .withOnCompletionActions(onCompletionActions)
                            .withJobManagerSharedServices(jobManagerSharedServices)
                            .createJobMaster();

            jobMaster.start();

            assertThat(onCompletionActions.getJobMasterFailedFuture().join())
                    .isInstanceOf(JobMasterException.class);

            // close the jobMaster to remove it from the testing rpc service so that it can shut
            // down cleanly
            try {
                jobMaster.close();
            } catch (Exception expected) {
                // expected
            }
        } finally {
            jobManagerSharedServices.shutdown();
        }
    }

    private static final class FailingSchedulerFactory implements SchedulerNGFactory {
        @Override
        public SchedulerNG createInstance(
                Logger log,
                ExecutionPlan executionPlan,
                Executor ioExecutor,
                Configuration jobMasterConfiguration,
                SlotPoolService slotPoolService,
                ScheduledExecutorService futureExecutor,
                ClassLoader userCodeLoader,
                CheckpointRecoveryFactory checkpointRecoveryFactory,
                Duration rpcTimeout,
                BlobWriter blobWriter,
                JobManagerJobMetricGroup jobManagerJobMetricGroup,
                Duration slotRequestTimeout,
                ShuffleMaster<?> shuffleMaster,
                JobMasterPartitionTracker partitionTracker,
                ExecutionDeploymentTracker executionDeploymentTracker,
                long initializationTimestamp,
                ComponentMainThreadExecutor mainThreadExecutor,
                FatalErrorHandler fatalErrorHandler,
                JobStatusListener jobStatusListener,
                Collection<FailureEnricher> failureEnrichers,
                BlocklistOperations blocklistOperations) {
            return TestingSchedulerNG.newBuilder()
                    .setStartSchedulingRunnable(
                            () -> {
                                throw new FlinkRuntimeException("Could not start scheduling.");
                            })
                    .build();
        }

        @Override
        public JobManagerOptions.SchedulerType getSchedulerType() {
            return JobManagerOptions.SchedulerType.Default;
        }
    }
}
