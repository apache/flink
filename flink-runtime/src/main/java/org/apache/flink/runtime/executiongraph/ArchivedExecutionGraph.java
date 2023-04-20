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

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/** An archived execution graph represents a serializable form of an {@link ExecutionGraph}. */
public class ArchivedExecutionGraph implements AccessExecutionGraph, Serializable {

    private static final long serialVersionUID = 7231383912742578428L;
    // --------------------------------------------------------------------------------------------

    /** The ID of the job this graph has been built for. */
    private final JobID jobID;

    /** The name of the original job graph. */
    private final String jobName;

    /** All job vertices that are part of this graph. */
    private final Map<JobVertexID, ArchivedExecutionJobVertex> tasks;

    /** All vertices, in the order in which they were created. * */
    private final List<ArchivedExecutionJobVertex> verticesInCreationOrder;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final long[] stateTimestamps;

    // ------ Configuration of the Execution -------

    // ------ Execution status and progress. These values are volatile, and accessed under the lock
    // -------

    /** Current status of the job execution. */
    private final JobStatus state;

    /**
     * The exception that caused the job to fail. This is set to the first root exception that was
     * not recoverable and triggered job failure
     */
    @Nullable private final ErrorInfo failureCause;

    // ------ Fields that are only relevant for archived execution graphs ------------
    private final String jsonPlan;
    private final StringifiedAccumulatorResult[] archivedUserAccumulators;
    private final ArchivedExecutionConfig archivedExecutionConfig;
    private final boolean isStoppable;
    private final Map<String, SerializedValue<OptionalFailure<Object>>> serializedUserAccumulators;

    @Nullable private final CheckpointCoordinatorConfiguration jobCheckpointingConfiguration;

    @Nullable private final CheckpointStatsSnapshot checkpointStatsSnapshot;

    @Nullable private final String stateBackendName;

    @Nullable private final String checkpointStorageName;

    @Nullable private final TernaryBoolean stateChangelogEnabled;

    @Nullable private final String changelogStorageName;

    public ArchivedExecutionGraph(
            JobID jobID,
            String jobName,
            Map<JobVertexID, ArchivedExecutionJobVertex> tasks,
            List<ArchivedExecutionJobVertex> verticesInCreationOrder,
            long[] stateTimestamps,
            JobStatus state,
            @Nullable ErrorInfo failureCause,
            String jsonPlan,
            StringifiedAccumulatorResult[] archivedUserAccumulators,
            Map<String, SerializedValue<OptionalFailure<Object>>> serializedUserAccumulators,
            ArchivedExecutionConfig executionConfig,
            boolean isStoppable,
            @Nullable CheckpointCoordinatorConfiguration jobCheckpointingConfiguration,
            @Nullable CheckpointStatsSnapshot checkpointStatsSnapshot,
            @Nullable String stateBackendName,
            @Nullable String checkpointStorageName,
            @Nullable TernaryBoolean stateChangelogEnabled,
            @Nullable String changelogStorageName) {

        this.jobID = Preconditions.checkNotNull(jobID);
        this.jobName = Preconditions.checkNotNull(jobName);
        this.tasks = Preconditions.checkNotNull(tasks);
        this.verticesInCreationOrder = Preconditions.checkNotNull(verticesInCreationOrder);
        this.stateTimestamps = Preconditions.checkNotNull(stateTimestamps);
        this.state = Preconditions.checkNotNull(state);
        this.failureCause = failureCause;
        this.jsonPlan = Preconditions.checkNotNull(jsonPlan);
        this.archivedUserAccumulators = Preconditions.checkNotNull(archivedUserAccumulators);
        this.serializedUserAccumulators = Preconditions.checkNotNull(serializedUserAccumulators);
        this.archivedExecutionConfig = Preconditions.checkNotNull(executionConfig);
        this.isStoppable = isStoppable;
        this.jobCheckpointingConfiguration = jobCheckpointingConfiguration;
        this.checkpointStatsSnapshot = checkpointStatsSnapshot;
        this.stateBackendName = stateBackendName;
        this.checkpointStorageName = checkpointStorageName;
        this.stateChangelogEnabled = stateChangelogEnabled;
        this.changelogStorageName = changelogStorageName;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String getJsonPlan() {
        return jsonPlan;
    }

    @Override
    public JobID getJobID() {
        return jobID;
    }

    @Override
    public String getJobName() {
        return jobName;
    }

    @Override
    public JobStatus getState() {
        return state;
    }

    @Nullable
    @Override
    public ErrorInfo getFailureInfo() {
        return failureCause;
    }

    @Override
    public ArchivedExecutionJobVertex getJobVertex(JobVertexID id) {
        return this.tasks.get(id);
    }

    @Override
    public Map<JobVertexID, AccessExecutionJobVertex> getAllVertices() {
        return Collections.<JobVertexID, AccessExecutionJobVertex>unmodifiableMap(this.tasks);
    }

    @Override
    public Iterable<ArchivedExecutionJobVertex> getVerticesTopologically() {
        // we return a specific iterator that does not fail with concurrent modifications
        // the list is append only, so it is safe for that
        final int numElements = this.verticesInCreationOrder.size();

        return new Iterable<ArchivedExecutionJobVertex>() {
            @Override
            public Iterator<ArchivedExecutionJobVertex> iterator() {
                return new Iterator<ArchivedExecutionJobVertex>() {
                    private int pos = 0;

                    @Override
                    public boolean hasNext() {
                        return pos < numElements;
                    }

                    @Override
                    public ArchivedExecutionJobVertex next() {
                        if (hasNext()) {
                            return verticesInCreationOrder.get(pos++);
                        } else {
                            throw new NoSuchElementException();
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public Iterable<ArchivedExecutionVertex> getAllExecutionVertices() {
        return new Iterable<ArchivedExecutionVertex>() {
            @Override
            public Iterator<ArchivedExecutionVertex> iterator() {
                return new AllVerticesIterator<>(getVerticesTopologically().iterator());
            }
        };
    }

    @Override
    public long getStatusTimestamp(JobStatus status) {
        return this.stateTimestamps[status.ordinal()];
    }

    @Override
    public CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration() {
        return jobCheckpointingConfiguration;
    }

    @Override
    public CheckpointStatsSnapshot getCheckpointStatsSnapshot() {
        return checkpointStatsSnapshot;
    }

    @Override
    public ArchivedExecutionConfig getArchivedExecutionConfig() {
        return archivedExecutionConfig;
    }

    @Override
    public boolean isStoppable() {
        return isStoppable;
    }

    @Override
    public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
        return archivedUserAccumulators;
    }

    @Override
    public Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized() {
        return serializedUserAccumulators;
    }

    @Override
    public Optional<String> getStateBackendName() {
        return Optional.ofNullable(stateBackendName);
    }

    @Override
    public Optional<String> getCheckpointStorageName() {
        return Optional.ofNullable(checkpointStorageName);
    }

    @Override
    public TernaryBoolean isChangelogStateBackendEnabled() {
        return stateChangelogEnabled;
    }

    @Override
    public Optional<String> getChangelogStorageName() {
        return Optional.ofNullable(changelogStorageName);
    }

    /**
     * Create a {@link ArchivedExecutionGraph} from the given {@link ExecutionGraph}.
     *
     * @param executionGraph to create the ArchivedExecutionGraph from
     * @return ArchivedExecutionGraph created from the given ExecutionGraph
     */
    public static ArchivedExecutionGraph createFrom(ExecutionGraph executionGraph) {
        return createFrom(executionGraph, null);
    }

    /**
     * Create a {@link ArchivedExecutionGraph} from the given {@link ExecutionGraph}.
     *
     * @param executionGraph to create the ArchivedExecutionGraph from
     * @param statusOverride optionally overrides the JobStatus of the ExecutionGraph with a
     *     non-globally-terminal state and clears timestamps of globally-terminal states
     * @return ArchivedExecutionGraph created from the given ExecutionGraph
     */
    public static ArchivedExecutionGraph createFrom(
            ExecutionGraph executionGraph, @Nullable JobStatus statusOverride) {
        Preconditions.checkArgument(
                statusOverride == null || !statusOverride.isGloballyTerminalState(),
                "Status override is only allowed for non-globally-terminal states.");

        Map<JobVertexID, ArchivedExecutionJobVertex> archivedTasks = new HashMap<>();
        List<ArchivedExecutionJobVertex> archivedVerticesInCreationOrder = new ArrayList<>();

        for (ExecutionJobVertex task : executionGraph.getVerticesTopologically()) {
            ArchivedExecutionJobVertex archivedTask = task.archive();
            archivedVerticesInCreationOrder.add(archivedTask);
            archivedTasks.put(task.getJobVertexId(), archivedTask);
        }

        final Map<String, SerializedValue<OptionalFailure<Object>>> serializedUserAccumulators =
                executionGraph.getAccumulatorsSerialized();

        final long[] timestamps = new long[JobStatus.values().length];

        // if the state is overridden with a non-globally-terminal state then we need to erase
        // traces of globally-terminal states for consistency
        final boolean clearGloballyTerminalStateTimestamps = statusOverride != null;

        for (JobStatus jobStatus : JobStatus.values()) {
            final int ordinal = jobStatus.ordinal();
            if (!(clearGloballyTerminalStateTimestamps && jobStatus.isGloballyTerminalState())) {
                timestamps[ordinal] = executionGraph.getStatusTimestamp(jobStatus);
            }
        }

        return new ArchivedExecutionGraph(
                executionGraph.getJobID(),
                executionGraph.getJobName(),
                archivedTasks,
                archivedVerticesInCreationOrder,
                timestamps,
                statusOverride == null ? executionGraph.getState() : statusOverride,
                executionGraph.getFailureInfo(),
                executionGraph.getJsonPlan(),
                executionGraph.getAccumulatorResultsStringified(),
                serializedUserAccumulators,
                executionGraph.getArchivedExecutionConfig(),
                executionGraph.isStoppable(),
                executionGraph.getCheckpointCoordinatorConfiguration(),
                executionGraph.getCheckpointStatsSnapshot(),
                executionGraph.getStateBackendName().orElse(null),
                executionGraph.getCheckpointStorageName().orElse(null),
                executionGraph.isChangelogStateBackendEnabled(),
                executionGraph.getChangelogStorageName().orElse(null));
    }

    /**
     * Create a sparse ArchivedExecutionGraph for a job. Most fields will be empty, only job status
     * and error-related fields are set.
     */
    public static ArchivedExecutionGraph createSparseArchivedExecutionGraph(
            JobID jobId,
            String jobName,
            JobStatus jobStatus,
            @Nullable Throwable throwable,
            @Nullable JobCheckpointingSettings checkpointingSettings,
            long initializationTimestamp) {
        return createSparseArchivedExecutionGraph(
                jobId,
                jobName,
                jobStatus,
                Collections.emptyMap(),
                Collections.emptyList(),
                throwable,
                checkpointingSettings,
                initializationTimestamp);
    }

    public static ArchivedExecutionGraph createSparseArchivedExecutionGraphWithJobVertices(
            JobID jobId,
            String jobName,
            JobStatus jobStatus,
            @Nullable Throwable throwable,
            @Nullable JobCheckpointingSettings checkpointingSettings,
            long initializationTimestamp,
            Iterable<JobVertex> jobVertices,
            VertexParallelismStore initialParallelismStore) {
        final Map<JobVertexID, ArchivedExecutionJobVertex> archivedJobVertices = new HashMap<>();
        final List<ArchivedExecutionJobVertex> archivedVerticesInCreationOrder = new ArrayList<>();
        for (JobVertex jobVertex : jobVertices) {
            final VertexParallelismInformation parallelismInfo =
                    initialParallelismStore.getParallelismInfo(jobVertex.getID());

            ArchivedExecutionJobVertex archivedJobVertex =
                    new ArchivedExecutionJobVertex(
                            new ArchivedExecutionVertex[0],
                            jobVertex.getID(),
                            jobVertex.getName(),
                            parallelismInfo.getParallelism(),
                            parallelismInfo.getMaxParallelism(),
                            ResourceProfile.fromResourceSpec(
                                    jobVertex.getMinResources(), MemorySize.ZERO),
                            new StringifiedAccumulatorResult[0]);
            archivedVerticesInCreationOrder.add(archivedJobVertex);
            archivedJobVertices.put(archivedJobVertex.getJobVertexId(), archivedJobVertex);
        }
        return createSparseArchivedExecutionGraph(
                jobId,
                jobName,
                jobStatus,
                archivedJobVertices,
                archivedVerticesInCreationOrder,
                throwable,
                checkpointingSettings,
                initializationTimestamp);
    }

    private static ArchivedExecutionGraph createSparseArchivedExecutionGraph(
            JobID jobId,
            String jobName,
            JobStatus jobStatus,
            Map<JobVertexID, ArchivedExecutionJobVertex> archivedTasks,
            List<ArchivedExecutionJobVertex> archivedVerticesInCreationOrder,
            @Nullable Throwable throwable,
            @Nullable JobCheckpointingSettings checkpointingSettings,
            long initializationTimestamp) {
        final Map<String, SerializedValue<OptionalFailure<Object>>> serializedUserAccumulators =
                Collections.emptyMap();
        StringifiedAccumulatorResult[] archivedUserAccumulators =
                new StringifiedAccumulatorResult[] {};

        final long[] timestamps = new long[JobStatus.values().length];
        timestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;

        String jsonPlan = "{}";

        ErrorInfo failureInfo = null;
        if (throwable != null) {
            Preconditions.checkState(
                    jobStatus == JobStatus.FAILED || jobStatus == JobStatus.SUSPENDED);
            long failureTime = System.currentTimeMillis();
            failureInfo = new ErrorInfo(throwable, failureTime);
            timestamps[jobStatus.ordinal()] = failureTime;
        }

        return new ArchivedExecutionGraph(
                jobId,
                jobName,
                archivedTasks,
                archivedVerticesInCreationOrder,
                timestamps,
                jobStatus,
                failureInfo,
                jsonPlan,
                archivedUserAccumulators,
                serializedUserAccumulators,
                new ExecutionConfig().archive(),
                false,
                checkpointingSettings == null
                        ? null
                        : checkpointingSettings.getCheckpointCoordinatorConfiguration(),
                checkpointingSettings == null ? null : CheckpointStatsSnapshot.empty(),
                checkpointingSettings == null ? null : "Unknown",
                checkpointingSettings == null ? null : "Unknown",
                checkpointingSettings == null
                        ? TernaryBoolean.UNDEFINED
                        : checkpointingSettings.isChangelogStateBackendEnabled(),
                checkpointingSettings == null ? null : "Unknown");
    }
}
