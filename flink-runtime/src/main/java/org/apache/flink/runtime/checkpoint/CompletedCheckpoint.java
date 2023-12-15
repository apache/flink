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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.FutureUtils.ConjunctFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A CompletedCheckpoint describes a checkpoint after all required tasks acknowledged it (with their
 * state) and that is considered successful. The CompletedCheckpoint class contains all the metadata
 * of the checkpoint, i.e., checkpoint ID, timestamps, and the handles to all states that are part
 * of the checkpoint.
 *
 * <h2>Size the CompletedCheckpoint Instances</h2>
 *
 * <p>In most cases, the CompletedCheckpoint objects are very small, because the handles to the
 * checkpoint states are only pointers (such as file paths). However, the some state backend
 * implementations may choose to store some payload data directly with the metadata (for example to
 * avoid many small files). If those thresholds are increased to large values, the memory
 * consumption of the CompletedCheckpoint objects can be significant.
 *
 * <h2>Metadata Persistence</h2>
 *
 * <p>The metadata of the CompletedCheckpoint is also persisted in an external storage system.
 * Checkpoints have an external pointer, which points to the metadata. For example when storing a
 * checkpoint in a file system, that pointer is the file path to the checkpoint's folder or the
 * metadata file. For a state backend that stores metadata in database tables, the pointer could be
 * the table name and row key. The pointer is encoded as a String.
 */
@NotThreadSafe
public class CompletedCheckpoint implements Serializable, Checkpoint {

    private static final Logger LOG = LoggerFactory.getLogger(CompletedCheckpoint.class);

    private static final long serialVersionUID = -8360248179615702014L;

    // ------------------------------------------------------------------------

    /** The ID of the job that the checkpoint belongs to. */
    private final JobID job;

    /** The ID (logical timestamp) of the checkpoint. */
    private final long checkpointID;

    /** The timestamp when the checkpoint was triggered. */
    private final long timestamp;

    /** The timestamp when the checkpoint was completed. */
    private final long completionTimestamp;

    /** States of the different operator groups belonging to this checkpoint. */
    private final Map<OperatorID, OperatorState> operatorStates;

    /** Properties of this checkpoint. Might change during recovery. */
    private final CheckpointProperties props;

    /**
     * Properties of this checkpoint as they were during checkpoint creation. Might be null for
     * older versions.
     */
    @Nullable private final CheckpointProperties restoredProps;

    /** States that were created by a hook on the master (in the checkpoint coordinator). */
    private final Collection<MasterState> masterHookStates;

    /** The location where the checkpoint is stored. */
    private final CompletedCheckpointStorageLocation storageLocation;

    /** The state handle to the externalized meta data. */
    private final StreamStateHandle metadataHandle;

    /** External pointer to the completed checkpoint (for example file path). */
    private final String externalPointer;

    /** Completed statistic for managing discard marker. */
    @Nullable private final transient CompletedCheckpointStats completedCheckpointStats;

    // ------------------------------------------------------------------------

    public CompletedCheckpoint(
            JobID job,
            long checkpointID,
            long timestamp,
            long completionTimestamp,
            Map<OperatorID, OperatorState> operatorStates,
            @Nullable Collection<MasterState> masterHookStates,
            CheckpointProperties props,
            CompletedCheckpointStorageLocation storageLocation,
            @Nullable CompletedCheckpointStats completedCheckpointStats) {
        this(
                job,
                checkpointID,
                timestamp,
                completionTimestamp,
                operatorStates,
                masterHookStates,
                props,
                storageLocation,
                completedCheckpointStats,
                null);
    }

    public CompletedCheckpoint(
            JobID job,
            long checkpointID,
            long timestamp,
            long completionTimestamp,
            Map<OperatorID, OperatorState> operatorStates,
            @Nullable Collection<MasterState> masterHookStates,
            CheckpointProperties props,
            CompletedCheckpointStorageLocation storageLocation,
            @Nullable CompletedCheckpointStats completedCheckpointStats,
            @Nullable CheckpointProperties restoredProps) {

        checkArgument(checkpointID >= 0);
        checkArgument(timestamp >= 0);
        checkArgument(completionTimestamp >= 0);

        this.job = checkNotNull(job);
        this.checkpointID = checkpointID;
        this.timestamp = timestamp;
        this.completionTimestamp = completionTimestamp;

        // we create copies here, to make sure we have no shared mutable
        // data structure with the "outside world"
        this.operatorStates = new HashMap<>(checkNotNull(operatorStates));
        this.masterHookStates =
                masterHookStates == null || masterHookStates.isEmpty()
                        ? Collections.emptyList()
                        : new ArrayList<>(masterHookStates);

        this.props = checkNotNull(props);
        this.storageLocation = checkNotNull(storageLocation);
        this.metadataHandle = storageLocation.getMetadataHandle();
        this.externalPointer = storageLocation.getExternalPointer();
        this.completedCheckpointStats = completedCheckpointStats;
        this.restoredProps = restoredProps;
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public JobID getJobId() {
        return job;
    }

    @Override
    public long getCheckpointID() {
        return checkpointID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getCompletionTimestamp() {
        return completionTimestamp;
    }

    public CheckpointProperties getProperties() {
        return props;
    }

    public Optional<CheckpointProperties> getRestoredProperties() {
        return Optional.ofNullable(restoredProps);
    }

    public Map<OperatorID, OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public Collection<MasterState> getMasterHookStates() {
        return Collections.unmodifiableCollection(masterHookStates);
    }

    public StreamStateHandle getMetadataHandle() {
        return metadataHandle;
    }

    public String getExternalPointer() {
        return externalPointer;
    }

    public long getStateSize() {
        long result = 0L;

        for (OperatorState operatorState : operatorStates.values()) {
            result += operatorState.getStateSize();
        }

        return result;
    }

    // ------------------------------------------------------------------------
    //  Shared State
    // ------------------------------------------------------------------------

    /**
     * Register all shared states in the given registry. This method is called before the checkpoint
     * is added into the store.
     *
     * @param sharedStateRegistry The registry where shared states are registered
     * @param restoreMode the mode in which this checkpoint was restored from
     */
    public void registerSharedStatesAfterRestored(
            SharedStateRegistry sharedStateRegistry, RestoreMode restoreMode) {
        // in claim mode we should not register any shared handles
        if (!props.isUnclaimed()) {
            sharedStateRegistry.registerAllAfterRestored(this, restoreMode);
        }
    }

    // ------------------------------------------------------------------------
    //  Discard and Dispose
    // ------------------------------------------------------------------------

    public DiscardObject markAsDiscarded() {
        if (completedCheckpointStats != null) {
            completedCheckpointStats.discard();
        }

        return new CompletedCheckpointDiscardObject();
    }

    public DiscardObject markAsDiscardedOnSubsume() {
        return shouldBeDiscardedOnSubsume() ? markAsDiscarded() : NOOP_DISCARD_OBJECT;
    }

    public DiscardObject markAsDiscardedOnShutdown(JobStatus jobStatus) {
        return shouldBeDiscardedOnShutdown(jobStatus) ? markAsDiscarded() : NOOP_DISCARD_OBJECT;
    }

    public boolean shouldBeDiscardedOnSubsume() {
        return props.discardOnSubsumed();
    }

    public boolean shouldBeDiscardedOnShutdown(JobStatus jobStatus) {
        return jobStatus == JobStatus.FINISHED && props.discardOnJobFinished()
                || jobStatus == JobStatus.CANCELED && props.discardOnJobCancelled()
                || jobStatus == JobStatus.FAILED && props.discardOnJobFailed()
                || jobStatus == JobStatus.SUSPENDED && props.discardOnJobSuspended();
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------

    public static boolean checkpointsMatch(
            Collection<CompletedCheckpoint> first, Collection<CompletedCheckpoint> second) {
        if (first.size() != second.size()) {
            return false;
        }

        List<Tuple2<Long, JobID>> firstInterestingFields = new ArrayList<>(first.size());

        for (CompletedCheckpoint checkpoint : first) {
            firstInterestingFields.add(
                    new Tuple2<>(checkpoint.getCheckpointID(), checkpoint.getJobId()));
        }

        List<Tuple2<Long, JobID>> secondInterestingFields = new ArrayList<>(second.size());

        for (CompletedCheckpoint checkpoint : second) {
            secondInterestingFields.add(
                    new Tuple2<>(checkpoint.getCheckpointID(), checkpoint.getJobId()));
        }

        return firstInterestingFields.equals(secondInterestingFields);
    }

    @Nullable
    public CompletedCheckpointStats getStatistic() {
        return completedCheckpointStats;
    }

    @Override
    public String toString() {
        return String.format(
                "%s %d @ %d for %s located at %s",
                props.getCheckpointType().getName(), checkpointID, timestamp, job, externalPointer);
    }

    /** Implementation of {@link org.apache.flink.runtime.checkpoint.Checkpoint.DiscardObject}. */
    @NotThreadSafe
    public class CompletedCheckpointDiscardObject implements DiscardObject {
        @Override
        public void discard() throws Exception {
            LOG.trace("Executing discard procedure for {}.", this);
            checkState(
                    isMarkedAsDiscarded(),
                    "Checkpoint should be marked as discarded before discard.");

            try {
                // collect exceptions and continue cleanup
                Exception exception = null;

                // drop the metadata
                try {
                    metadataHandle.discardState();
                } catch (Exception e) {
                    exception = e;
                }

                // discard private state objects
                try {
                    StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }

                // discard location as a whole
                try {
                    storageLocation.disposeStorageLocation();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }

                if (exception != null) {
                    throw exception;
                }
            } finally {
                operatorStates.clear();
            }
        }

        private boolean isMarkedAsDiscarded() {
            return completedCheckpointStats == null || completedCheckpointStats.isDiscarded();
        }

        @Override
        public CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            checkState(
                    isMarkedAsDiscarded(),
                    "Checkpoint should be marked as discarded before discard.");

            List<StateObject> discardables =
                    operatorStates.values().stream()
                            .flatMap(op -> op.getDiscardables().stream())
                            .collect(Collectors.toList());
            discardables.add(metadataHandle);

            ConjunctFuture<Void> discardStates =
                    FutureUtils.completeAll(
                            discardables.stream()
                                    .map(
                                            item ->
                                                    FutureUtils.runAsync(
                                                            item::discardState, ioExecutor))
                                    .collect(Collectors.toList()));

            return FutureUtils.runAfterwards(
                    discardStates,
                    () -> {
                        operatorStates.clear();
                        storageLocation.disposeStorageLocation();
                    });
        }
    }
}
