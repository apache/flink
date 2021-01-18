/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentTracker;
import org.apache.flink.runtime.scheduler.DefaultExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.VertexwiseSchedulingStrategy;

import java.util.concurrent.ScheduledExecutorService;

/** A utility class to create {@link AdaptiveBatchScheduler} instances for testing. */
public class AdaptiveBatchSchedulerTestUtils {

    /** Builder for {@link AdaptiveBatchScheduler}. */
    public static class AdaptiveBatchSchedulerBuilder
            extends SchedulerTestingUtils.DefaultSchedulerBuilder {

        private VertexParallelismDecider vertexParallelismDecider = (ignored) -> 0;

        private int defaultMaxParallelism =
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM.defaultValue();

        public AdaptiveBatchSchedulerBuilder(
                JobGraph jobGraph,
                ComponentMainThreadExecutor mainThreadExecutor,
                ScheduledExecutorService executorService) {
            super(jobGraph, mainThreadExecutor, executorService);
            setSchedulingStrategyFactory(new VertexwiseSchedulingStrategy.Factory());
        }

        public void setVertexParallelismDecider(VertexParallelismDecider vertexParallelismDecider) {
            this.vertexParallelismDecider = vertexParallelismDecider;
        }

        public void setDefaultMaxParallelism(int defaultMaxParallelism) {
            this.defaultMaxParallelism = defaultMaxParallelism;
        }

        @Override
        public AdaptiveBatchScheduler build() throws Exception {
            final ExecutionGraphFactory executionGraphFactory =
                    new DefaultExecutionGraphFactory(
                            jobMasterConfiguration,
                            userCodeLoader,
                            new DefaultExecutionDeploymentTracker(),
                            futureExecutor,
                            ioExecutor,
                            rpcTimeout,
                            jobManagerJobMetricGroup,
                            blobWriter,
                            shuffleMaster,
                            partitionTracker,
                            true);

            return new AdaptiveBatchScheduler(
                    log,
                    jobGraph,
                    ioExecutor,
                    jobMasterConfiguration,
                    componentMainThreadExecutor -> {},
                    delayExecutor,
                    userCodeLoader,
                    checkpointCleaner,
                    checkpointRecoveryFactory,
                    jobManagerJobMetricGroup,
                    schedulingStrategyFactory,
                    failoverStrategyFactory,
                    restartBackoffTimeStrategy,
                    executionVertexOperations,
                    executionVertexVersioner,
                    executionSlotAllocatorFactory,
                    System.currentTimeMillis(),
                    mainThreadExecutor,
                    jobStatusListener,
                    executionGraphFactory,
                    shuffleMaster,
                    rpcTimeout,
                    vertexParallelismDecider,
                    defaultMaxParallelism,
                    failureListeners);
        }
    }
}
