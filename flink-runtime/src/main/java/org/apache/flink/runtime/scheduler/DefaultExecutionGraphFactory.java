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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.VertexAttemptNumberStore;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTrackerDeploymentListenerAdapter;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.function.CachingSupplier;

import org.slf4j.Logger;

import java.util.HashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default {@link ExecutionGraphFactory} implementation. */
public class DefaultExecutionGraphFactory implements ExecutionGraphFactory {

    private final Configuration configuration;
    private final ClassLoader userCodeClassLoader;
    private final ExecutionDeploymentTracker executionDeploymentTracker;
    private final ScheduledExecutorService futureExecutor;
    private final Executor ioExecutor;
    private final Time rpcTimeout;
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;
    private final BlobWriter blobWriter;
    private final ShuffleMaster<?> shuffleMaster;
    private final JobMasterPartitionTracker jobMasterPartitionTracker;
    private final Supplier<CheckpointStatsTracker> checkpointStatsTrackerFactory;
    private final boolean isDynamicGraph;
    private final ExecutionJobVertex.Factory executionJobVertexFactory;

    private final boolean nonFinishedHybridPartitionShouldBeUnknown;

    public DefaultExecutionGraphFactory(
            Configuration configuration,
            ClassLoader userCodeClassLoader,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Time rpcTimeout,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            BlobWriter blobWriter,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker jobMasterPartitionTracker) {
        this(
                configuration,
                userCodeClassLoader,
                executionDeploymentTracker,
                futureExecutor,
                ioExecutor,
                rpcTimeout,
                jobManagerJobMetricGroup,
                blobWriter,
                shuffleMaster,
                jobMasterPartitionTracker,
                false,
                new ExecutionJobVertex.Factory(),
                false);
    }

    public DefaultExecutionGraphFactory(
            Configuration configuration,
            ClassLoader userCodeClassLoader,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Time rpcTimeout,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            BlobWriter blobWriter,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker jobMasterPartitionTracker,
            boolean isDynamicGraph,
            ExecutionJobVertex.Factory executionJobVertexFactory,
            boolean nonFinishedHybridPartitionShouldBeUnknown) {
        this.configuration = configuration;
        this.userCodeClassLoader = userCodeClassLoader;
        this.executionDeploymentTracker = executionDeploymentTracker;
        this.futureExecutor = futureExecutor;
        this.ioExecutor = ioExecutor;
        this.rpcTimeout = rpcTimeout;
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.blobWriter = blobWriter;
        this.shuffleMaster = shuffleMaster;
        this.jobMasterPartitionTracker = jobMasterPartitionTracker;
        this.checkpointStatsTrackerFactory =
                new CachingSupplier<>(
                        () ->
                                new CheckpointStatsTracker(
                                        configuration.getInteger(
                                                WebOptions.CHECKPOINTS_HISTORY_SIZE),
                                        jobManagerJobMetricGroup));
        this.isDynamicGraph = isDynamicGraph;
        this.executionJobVertexFactory = checkNotNull(executionJobVertexFactory);
        this.nonFinishedHybridPartitionShouldBeUnknown = nonFinishedHybridPartitionShouldBeUnknown;
    }

    @Override
    public ExecutionGraph createAndRestoreExecutionGraph(
            JobGraph jobGraph,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            long initializationTimestamp,
            VertexAttemptNumberStore vertexAttemptNumberStore,
            VertexParallelismStore vertexParallelismStore,
            ExecutionStateUpdateListener executionStateUpdateListener,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            Logger log)
            throws Exception {
        ExecutionDeploymentListener executionDeploymentListener =
                new ExecutionDeploymentTrackerDeploymentListenerAdapter(executionDeploymentTracker);
        ExecutionStateUpdateListener combinedExecutionStateUpdateListener =
                (execution, previousState, newState) -> {
                    executionStateUpdateListener.onStateUpdate(execution, previousState, newState);
                    if (newState.isTerminal()) {
                        executionDeploymentTracker.stopTrackingDeploymentOf(execution);
                    }
                };

        final ExecutionGraph newExecutionGraph =
                DefaultExecutionGraphBuilder.buildGraph(
                        jobGraph,
                        configuration,
                        futureExecutor,
                        ioExecutor,
                        userCodeClassLoader,
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        rpcTimeout,
                        blobWriter,
                        log,
                        shuffleMaster,
                        jobMasterPartitionTracker,
                        partitionLocationConstraint,
                        executionDeploymentListener,
                        combinedExecutionStateUpdateListener,
                        initializationTimestamp,
                        vertexAttemptNumberStore,
                        vertexParallelismStore,
                        checkpointStatsTrackerFactory,
                        isDynamicGraph,
                        executionJobVertexFactory,
                        markPartitionFinishedStrategy,
                        nonFinishedHybridPartitionShouldBeUnknown,
                        jobManagerJobMetricGroup);

        final CheckpointCoordinator checkpointCoordinator =
                newExecutionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator != null) {
            // check whether we find a valid checkpoint
            if (!checkpointCoordinator.restoreInitialCheckpointIfPresent(
                    new HashSet<>(newExecutionGraph.getAllVertices().values()))) {

                // check whether we can restore from a savepoint
                tryRestoreExecutionGraphFromSavepoint(
                        newExecutionGraph, jobGraph.getSavepointRestoreSettings());
            }
        }

        return newExecutionGraph;
    }

    /**
     * Tries to restore the given {@link ExecutionGraph} from the provided {@link
     * SavepointRestoreSettings}, iff checkpointing is enabled.
     *
     * @param executionGraphToRestore {@link ExecutionGraph} which is supposed to be restored
     * @param savepointRestoreSettings {@link SavepointRestoreSettings} containing information about
     *     the savepoint to restore from
     * @throws Exception if the {@link ExecutionGraph} could not be restored
     */
    private void tryRestoreExecutionGraphFromSavepoint(
            ExecutionGraph executionGraphToRestore,
            SavepointRestoreSettings savepointRestoreSettings)
            throws Exception {
        if (savepointRestoreSettings.restoreSavepoint()) {
            final CheckpointCoordinator checkpointCoordinator =
                    executionGraphToRestore.getCheckpointCoordinator();
            if (checkpointCoordinator != null) {
                checkpointCoordinator.restoreSavepoint(
                        savepointRestoreSettings,
                        executionGraphToRestore.getAllVertices(),
                        userCodeClassLoader);
            }
        }
    }
}
