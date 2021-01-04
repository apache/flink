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
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

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

/** An archived execution graph represents a serializable form of the {@link ExecutionGraph}. */
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
            @Nullable String stateBackendName) {

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
                return new AllVerticesIterator(getVerticesTopologically().iterator());
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
    public boolean isArchived() {
        return true;
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

    class AllVerticesIterator implements Iterator<ArchivedExecutionVertex> {

        private final Iterator<ArchivedExecutionJobVertex> jobVertices;

        private ArchivedExecutionVertex[] currVertices;

        private int currPos;

        public AllVerticesIterator(Iterator<ArchivedExecutionJobVertex> jobVertices) {
            this.jobVertices = jobVertices;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currVertices != null) {
                    if (currPos < currVertices.length) {
                        return true;
                    } else {
                        currVertices = null;
                    }
                } else if (jobVertices.hasNext()) {
                    currVertices = jobVertices.next().getTaskVertices();
                    currPos = 0;
                } else {
                    return false;
                }
            }
        }

        @Override
        public ArchivedExecutionVertex next() {
            if (hasNext()) {
                return currVertices[currPos++];
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Create a {@link ArchivedExecutionGraph} from the given {@link ExecutionGraph}.
     *
     * @param executionGraph to create the ArchivedExecutionGraph from
     * @return ArchivedExecutionGraph created from the given ExecutionGraph
     */
    public static ArchivedExecutionGraph createFrom(ExecutionGraph executionGraph) {
        final int numberVertices = executionGraph.getTotalNumberOfVertices();

        Map<JobVertexID, ArchivedExecutionJobVertex> archivedTasks = new HashMap<>(numberVertices);
        List<ArchivedExecutionJobVertex> archivedVerticesInCreationOrder =
                new ArrayList<>(numberVertices);

        for (ExecutionJobVertex task : executionGraph.getVerticesTopologically()) {
            ArchivedExecutionJobVertex archivedTask = task.archive();
            archivedVerticesInCreationOrder.add(archivedTask);
            archivedTasks.put(task.getJobVertexId(), archivedTask);
        }

        final Map<String, SerializedValue<OptionalFailure<Object>>> serializedUserAccumulators =
                executionGraph.getAccumulatorsSerialized();

        final long[] timestamps = new long[JobStatus.values().length];

        for (JobStatus jobStatus : JobStatus.values()) {
            final int ordinal = jobStatus.ordinal();
            timestamps[ordinal] = executionGraph.getStatusTimestamp(jobStatus);
        }

        return new ArchivedExecutionGraph(
                executionGraph.getJobID(),
                executionGraph.getJobName(),
                archivedTasks,
                archivedVerticesInCreationOrder,
                timestamps,
                executionGraph.getState(),
                executionGraph.getFailureInfo(),
                executionGraph.getJsonPlan(),
                executionGraph.getAccumulatorResultsStringified(),
                serializedUserAccumulators,
                executionGraph.getArchivedExecutionConfig(),
                executionGraph.isStoppable(),
                executionGraph.getCheckpointCoordinatorConfiguration(),
                executionGraph.getCheckpointStatsSnapshot(),
                executionGraph.getStateBackendName().orElse(null));
    }

    /**
     * Create a sparse ArchivedExecutionGraph for a job while it is still initializing. Most fields
     * will be empty, only job status and error-related fields are set.
     */
    public static ArchivedExecutionGraph createFromInitializingJob(
            JobID jobId,
            String jobName,
            JobStatus jobStatus,
            @Nullable Throwable throwable,
            long initializationTimestamp) {
        Map<JobVertexID, ArchivedExecutionJobVertex> archivedTasks = Collections.emptyMap();
        List<ArchivedExecutionJobVertex> archivedVerticesInCreationOrder = Collections.emptyList();
        final Map<String, SerializedValue<OptionalFailure<Object>>> serializedUserAccumulators =
                Collections.emptyMap();
        StringifiedAccumulatorResult[] archivedUserAccumulators =
                new StringifiedAccumulatorResult[] {};

        final long[] timestamps = new long[JobStatus.values().length];
        timestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;

        String jsonPlan = "{}";

        ErrorInfo failureInfo = null;
        if (throwable != null) {
            Preconditions.checkState(jobStatus == JobStatus.FAILED);
            long failureTime = System.currentTimeMillis();
            failureInfo = new ErrorInfo(throwable, failureTime);
            timestamps[JobStatus.FAILED.ordinal()] = failureTime;
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
                null,
                null,
                null);
    }
}
