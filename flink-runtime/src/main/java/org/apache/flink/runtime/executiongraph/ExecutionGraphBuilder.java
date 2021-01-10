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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link ExecutionGraph} from a {@link
 * JobGraph}.
 */
public class ExecutionGraphBuilder {

    public static ExecutionGraph buildGraph(
            JobGraph jobGraph,
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            SlotProvider slotProvider,
            ClassLoader classLoader,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            Time rpcTimeout,
            MetricGroup metrics,
            BlobWriter blobWriter,
            Time allocationTimeout,
            Logger log,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp)
            throws JobExecutionException, JobException {

        checkNotNull(jobGraph, "job graph cannot be null");

        final String jobName = jobGraph.getName();
        final JobID jobId = jobGraph.getJobID();

        final JobInformation jobInformation =
                new JobInformation(
                        jobId,
                        jobName,
                        jobGraph.getSerializedExecutionConfig(),
                        jobGraph.getJobConfiguration(),
                        jobGraph.getUserJarBlobKeys(),
                        jobGraph.getClasspaths());

        final int maxPriorAttemptsHistoryLength =
                jobManagerConfig.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

        final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory =
                PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(
                        jobManagerConfig);

        // create a new execution graph, if none exists so far
        final ExecutionGraph executionGraph;
        try {
            executionGraph =
                    new ExecutionGraph(
                            jobInformation,
                            futureExecutor,
                            ioExecutor,
                            rpcTimeout,
                            maxPriorAttemptsHistoryLength,
                            slotProvider,
                            classLoader,
                            blobWriter,
                            allocationTimeout,
                            partitionReleaseStrategyFactory,
                            shuffleMaster,
                            partitionTracker,
                            jobGraph.getScheduleMode(),
                            executionDeploymentListener,
                            executionStateUpdateListener,
                            initializationTimestamp);
        } catch (IOException e) {
            throw new JobException("Could not create the ExecutionGraph.", e);
        }

        // set the basic properties

        try {
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch (Throwable t) {
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            executionGraph.setJsonPlan("{}");
        }

        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits

        final long initMasterStart = System.nanoTime();
        log.info("Running initialization on master for job {} ({}).", jobName, jobId);

        for (JobVertex vertex : jobGraph.getVertices()) {
            String executableClass = vertex.getInvokableClassName();
            if (executableClass == null || executableClass.isEmpty()) {
                throw new JobSubmissionException(
                        jobId,
                        "The vertex "
                                + vertex.getID()
                                + " ("
                                + vertex.getName()
                                + ") has no invokable class.");
            }

            try {
                vertex.initializeOnMaster(classLoader);
            } catch (Throwable t) {
                throw new JobExecutionException(
                        jobId,
                        "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(),
                        t);
            }
        }

        log.info(
                "Successfully ran initialization on master in {} ms.",
                (System.nanoTime() - initMasterStart) / 1_000_000);

        // topologically sort the job vertices and attach the graph to the existing one
        List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Adding {} vertices from job graph {} ({}).",
                    sortedTopology.size(),
                    jobName,
                    jobId);
        }
        executionGraph.attachJobGraph(sortedTopology);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Successfully created execution graph from job graph {} ({}).", jobName, jobId);
        }

        // configure the state checkpointing
        if (isCheckpointingEnabled(jobGraph)) {
            JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();
            List<ExecutionJobVertex> triggerVertices =
                    idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

            List<ExecutionJobVertex> ackVertices =
                    idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

            List<ExecutionJobVertex> confirmVertices =
                    idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);

            // Maximum number of remembered checkpoints
            int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

            CheckpointStatsTracker checkpointStatsTracker =
                    new CheckpointStatsTracker(
                            historySize,
                            ackVertices,
                            snapshotSettings.getCheckpointCoordinatorConfiguration(),
                            metrics);

            // load the state backend from the application settings
            final StateBackend applicationConfiguredBackend;
            final SerializedValue<StateBackend> serializedAppConfigured =
                    snapshotSettings.getDefaultStateBackend();

            if (serializedAppConfigured == null) {
                applicationConfiguredBackend = null;
            } else {
                try {
                    applicationConfiguredBackend =
                            serializedAppConfigured.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not deserialize application-defined state backend.", e);
                }
            }

            final StateBackend rootBackend;
            try {
                rootBackend =
                        StateBackendLoader.fromApplicationOrConfigOrDefault(
                                applicationConfiguredBackend, jobManagerConfig, classLoader, log);
            } catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured state backend", e);
            }

            // instantiate the user-defined checkpoint hooks

            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks =
                    snapshotSettings.getMasterHooks();
            final List<MasterTriggerRestoreHook<?>> hooks;

            if (serializedHooks == null) {
                hooks = Collections.emptyList();
            } else {
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }

                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);

                try {
                    hooks = new ArrayList<>(hookFactories.length);
                    for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }
            }

            final CheckpointCoordinatorConfiguration chkConfig =
                    snapshotSettings.getCheckpointCoordinatorConfiguration();

            executionGraph.enableCheckpointing(
                    chkConfig,
                    triggerVertices,
                    ackVertices,
                    confirmVertices,
                    hooks,
                    checkpointIdCounter,
                    completedCheckpointStore,
                    rootBackend,
                    checkpointStatsTracker,
                    checkpointsCleaner);
        }

        // create all the metrics for the Execution Graph

        metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
        metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
        metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));

        return executionGraph;
    }

    public static boolean isCheckpointingEnabled(JobGraph jobGraph) {
        return jobGraph.getCheckpointingSettings() != null;
    }

    private static List<ExecutionJobVertex> idToVertex(
            List<JobVertexID> jobVertices, ExecutionGraph executionGraph)
            throws IllegalArgumentException {

        List<ExecutionJobVertex> result = new ArrayList<>(jobVertices.size());

        for (JobVertexID id : jobVertices) {
            ExecutionJobVertex vertex = executionGraph.getJobVertex(id);
            if (vertex != null) {
                result.add(vertex);
            } else {
                throw new IllegalArgumentException(
                        "The snapshot checkpointing settings refer to non-existent vertex " + id);
            }
        }

        return result;
    }

    // ------------------------------------------------------------------------

    /** This class is not supposed to be instantiated. */
    private ExecutionGraphBuilder() {}
}
