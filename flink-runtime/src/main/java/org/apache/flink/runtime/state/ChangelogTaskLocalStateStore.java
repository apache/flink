/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR;
import static org.apache.flink.util.Preconditions.checkState;

/** Changelog's implementation of a {@link TaskLocalStateStore}. */
public class ChangelogTaskLocalStateStore extends TaskLocalStateStoreImpl {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogTaskLocalStateStore.class);

    private static final String CHANGE_LOG_CHECKPOINT_PREFIX = "changelog_chk_";

    /**
     * The mapper of checkpointId and materializationId. (cp3, materializationId2) means cp3 refer
     * to m1.
     */
    private final Map<Long, Long> mapToMaterializationId;

    /** Last checkpointId, to check whether checkpoint is out of order. */
    private long lastCheckpointId = -1L;

    public ChangelogTaskLocalStateStore(
            @Nonnull JobID jobID,
            @Nonnull AllocationID allocationID,
            @Nonnull JobVertexID jobVertexID,
            @Nonnegative int subtaskIndex,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull Executor discardExecutor) {
        super(jobID, allocationID, jobVertexID, subtaskIndex, localRecoveryConfig, discardExecutor);
        this.mapToMaterializationId = new HashMap<>();
    }

    private void updateReference(long checkpointId, TaskStateSnapshot localState) {
        if (localState == null) {
            localState = NULL_DUMMY;
        }
        for (Map.Entry<OperatorID, OperatorSubtaskState> subtaskStateEntry :
                localState.getSubtaskStateMappings()) {
            for (KeyedStateHandle keyedStateHandle :
                    subtaskStateEntry.getValue().getManagedKeyedState()) {
                if (keyedStateHandle instanceof ChangelogStateBackendHandle) {
                    ChangelogStateBackendHandle changelogStateBackendHandle =
                            (ChangelogStateBackendHandle) keyedStateHandle;
                    long materializationID = changelogStateBackendHandle.getMaterializationID();
                    if (mapToMaterializationId.containsKey(checkpointId)) {
                        checkState(
                                materializationID == mapToMaterializationId.get(checkpointId),
                                "one checkpoint contains at most one materializationID");
                    } else {
                        mapToMaterializationId.put(checkpointId, materializationID);
                    }
                }
            }
        }
    }

    public static Path getLocalTaskOwnedDirectory(
            LocalRecoveryDirectoryProvider provider, JobID jobID) {
        File outDir =
                provider.selectAllocationBaseDirectory(
                        (jobID.hashCode() & Integer.MAX_VALUE)
                                % provider.allocationBaseDirsCount());
        if (!outDir.exists() && !outDir.mkdirs()) {
            LOG.error(
                    "Local state base directory does not exist and could not be created: "
                            + outDir);
        }
        return new Path(
                String.format("%s/jid_%s", outDir.toURI(), jobID), CHECKPOINT_TASK_OWNED_STATE_DIR);
    }

    @Override
    public void storeLocalState(long checkpointId, @Nullable TaskStateSnapshot localState) {
        if (checkpointId < lastCheckpointId) {
            LOG.info(
                    "Current checkpoint {} is out of order, smaller than last CheckpointId {}.",
                    lastCheckpointId,
                    checkpointId);
            return;
        } else {
            lastCheckpointId = checkpointId;
        }
        synchronized (lock) {
            updateReference(checkpointId, localState);
        }
        super.storeLocalState(checkpointId, localState);
    }

    @Override
    protected File getCheckpointDirectory(long checkpointId) {
        return new File(
                getLocalRecoveryDirectoryProvider().subtaskBaseDirectory(checkpointId),
                CHANGE_LOG_CHECKPOINT_PREFIX + checkpointId);
    }

    private void deleteMaterialization(LongPredicate pruningChecker) {
        Set<Long> materializationToRemove;
        synchronized (lock) {
            Set<Long> checkpoints =
                    mapToMaterializationId.keySet().stream()
                            .filter(pruningChecker::test)
                            .collect(Collectors.toSet());
            materializationToRemove =
                    checkpoints.stream()
                            .map(mapToMaterializationId::remove)
                            .collect(Collectors.toSet());
            materializationToRemove.removeAll(mapToMaterializationId.values());
        }

        discardExecutor.execute(
                () ->
                        syncDiscardFileForCollection(
                                materializationToRemove.stream()
                                        .map(super::getCheckpointDirectory)
                                        .collect(Collectors.toList())));
    }

    private void syncDiscardFileForCollection(Collection<File> toDiscard) {
        for (File directory : toDiscard) {
            if (directory.exists()) {
                try {
                    // TODO: This is guaranteed by the wrapped backend only using this folder for
                    // its local state, the materialized handle should be discarded here too.
                    deleteDirectory(directory);
                } catch (IOException ex) {
                    LOG.warn(
                            "Exception while deleting local state directory of {} in subtask ({} - {} - {}).",
                            directory,
                            jobID,
                            jobVertexID,
                            subtaskIndex,
                            ex);
                }
            }
        }
    }

    @Override
    public void pruneCheckpoints(LongPredicate pruningChecker, boolean breakOnceCheckerFalse) {
        // Scenarios:
        //   c1,m1
        //   confirm c1, do nothing.
        //   c2,m1
        //   confirm c2, delete c1, don't delete m1
        //   c3,m2
        //   confirm c3, delete c2, delete m1

        // delete changelog-chk
        super.pruneCheckpoints(pruningChecker, false);
        deleteMaterialization(pruningChecker);
    }

    @Override
    public CompletableFuture<Void> dispose() {
        deleteMaterialization(id -> true);
        // delete all ChangelogStateHandle in taskowned directory.
        discardExecutor.execute(
                () ->
                        syncDiscardFileForCollection(
                                Collections.singleton(
                                        new File(
                                                getLocalTaskOwnedDirectory(
                                                                getLocalRecoveryDirectoryProvider(),
                                                                jobID)
                                                        .toUri()))));

        synchronized (lock) {
            mapToMaterializationId.clear();
        }
        return super.dispose();
    }

    @Override
    public String toString() {
        return "ChangelogTaskLocalStateStore{"
                + "jobID="
                + jobID
                + ", jobVertexID="
                + jobVertexID
                + ", allocationID="
                + allocationID.toHexString()
                + ", subtaskIndex="
                + subtaskIndex
                + ", localRecoveryConfig="
                + localRecoveryConfig
                + ", storedCheckpointIDs="
                + storedTaskStateByCheckpointID.keySet()
                + ", mapToMaterializationId="
                + mapToMaterializationId.entrySet()
                + '}';
    }
}
