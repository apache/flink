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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.NoOpExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.configuration.JobManagerOptions.PARTITION_RELEASE_DURING_JOB_EXECUTION;

/**
 * Mocked ExecutionGraph with the following properties:
 *
 * <ul>
 *   <li>it stays in CANCELLING, when cancel() gets called
 *   <li>it stays in FAILING then failJob() gets called
 *   <li>it leaves above states when completeCancellation() gets called.
 * </ul>
 */
class MockExecutionGraph extends ExecutionGraph {

    private final CompletableFuture<?> completeCancellationFuture = new CompletableFuture<>();
    private boolean isCancelling = false;
    private boolean isFailing = false;

    public MockExecutionGraph() throws IOException, JobException {

        super(
                new JobInformation(
                        new JobID(),
                        "Test Job",
                        new SerializedValue<>(new ExecutionConfig()),
                        new Configuration(),
                        Collections.emptyList(),
                        Collections.emptyList()),
                TestingUtils.defaultExecutor(),
                TestingUtils.defaultExecutor(),
                AkkaUtils.getDefaultTimeout(),
                1,
                ExecutionGraph.class.getClassLoader(),
                VoidBlobWriter.getInstance(),
                getPartitionReleaseStrategyFactoryLoader(),
                NettyShuffleMaster.INSTANCE,
                NoOpJobMasterPartitionTracker.INSTANCE,
                TaskDeploymentDescriptorFactory.PartitionLocationConstraint.MUST_BE_KNOWN,
                NoOpExecutionDeploymentListener.get(),
                (execution, newState) -> {},
                0L);
        this.setJsonPlan(""); // field must not be null for ArchivedExecutionGraph creation
        this.attachJobGraph(Collections.emptyList()); // method must be called to initialize
        // PartitionReleaseStrategy.
    }

    private static PartitionReleaseStrategy.Factory getPartitionReleaseStrategyFactoryLoader() {
        Configuration conf = new Configuration();
        conf.set(PARTITION_RELEASE_DURING_JOB_EXECUTION, false);
        return PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(conf);
    }

    void completeCancellation() {
        completeCancellationFuture.complete(null);
    }

    public boolean isCancelling() {
        return isCancelling;
    }

    public boolean isFailing() {
        return isFailing;
    }

    // overwrites for the tests
    @Override
    public void cancel() {
        super.cancel();
        this.isCancelling = true;
    }

    @Override
    public void failJob(Throwable cause) {
        super.failJob(cause);
        this.isFailing = true;
    }

    @Override
    protected FutureUtils.ConjunctFuture<Void> cancelVerticesAsync() {
        return FutureUtils.completeAll(Collections.singleton(completeCancellationFuture));
    }
}
