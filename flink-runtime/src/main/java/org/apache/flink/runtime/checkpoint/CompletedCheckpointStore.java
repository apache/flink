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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/** A bounded LIFO-queue of {@link CompletedCheckpoint} instances. */
public interface CompletedCheckpointStore {

    Logger LOG = LoggerFactory.getLogger(CompletedCheckpointStore.class);

    /**
     * Adds a {@link CompletedCheckpoint} instance to the list of completed checkpoints.
     *
     * <p>Only a bounded number of checkpoints is kept. When exceeding the maximum number of
     * retained checkpoints, the oldest one will be discarded.
     *
     * <p>After <a href="https://issues.apache.org/jira/browse/FLINK-24611">FLINK-24611</a>, {@link
     * SharedStateRegistry#unregisterUnusedState} should be called here to subsume unused state.
     * <font color="#FF0000"><strong>Note</strong></font>, the {@link CompletedCheckpoint} passed to
     * {@link SharedStateRegistry#registerAllAfterRestored} or {@link
     * SharedStateRegistryFactory#create} must be the same object as the input parameter, otherwise
     * the state may be deleted by mistake.
     *
     * <p>After <a href="https://issues.apache.org/jira/browse/FLINK-25872">FLINK-25872</a>, {@link
     * CheckpointsCleaner#cleanSubsumedCheckpoints} should be called explicitly here.
     *
     * @return the subsumed oldest completed checkpoint if possible, return null if no checkpoint
     *     needs to be discarded on subsume.
     */
    @Nullable
    CompletedCheckpoint addCheckpointAndSubsumeOldestOne(
            CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception;

    /**
     * Returns the latest {@link CompletedCheckpoint} instance or <code>null</code> if none was
     * added.
     */
    default CompletedCheckpoint getLatestCheckpoint() throws Exception {
        List<CompletedCheckpoint> allCheckpoints = getAllCheckpoints();
        if (allCheckpoints.isEmpty()) {
            return null;
        }

        return allCheckpoints.get(allCheckpoints.size() - 1);
    }

    /** Returns the id of the latest completed checkpoints. */
    default long getLatestCheckpointId() {
        try {
            List<CompletedCheckpoint> allCheckpoints = getAllCheckpoints();
            if (allCheckpoints.isEmpty()) {
                return 0;
            }

            return allCheckpoints.get(allCheckpoints.size() - 1).getCheckpointID();
        } catch (Throwable throwable) {
            LOG.warn("Get the latest completed checkpoints failed", throwable);
            return 0;
        }
    }

    /**
     * Shuts down the store.
     *
     * <p>The job status is forwarded and used to decide whether state should actually be discarded
     * or kept. {@link SharedStateRegistry#unregisterUnusedState} and {@link
     * CheckpointsCleaner#cleanSubsumedCheckpoints} should be called here to subsume unused state.
     *
     * @param jobStatus Job state on shut down
     * @param checkpointsCleaner that will cleanup completed checkpoints if needed
     */
    void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner) throws Exception;

    /**
     * Returns all {@link CompletedCheckpoint} instances.
     *
     * <p>Returns an empty list if no checkpoint has been added yet.
     */
    List<CompletedCheckpoint> getAllCheckpoints() throws Exception;

    /** Returns the current number of retained checkpoints. */
    int getNumberOfRetainedCheckpoints();

    /** Returns the max number of retained checkpoints. */
    int getMaxNumberOfRetainedCheckpoints();

    /**
     * This method returns whether the completed checkpoint store requires checkpoints to be
     * externalized. Externalized checkpoints have their meta data persisted, which the checkpoint
     * store can exploit (for example by simply pointing the persisted metadata).
     *
     * @return True, if the store requires that checkpoints are externalized before being added,
     *     false if the store stores the metadata itself.
     */
    boolean requiresExternalizedCheckpoints();

    /** Returns the {@link SharedStateRegistry} used to register the shared state. */
    SharedStateRegistry getSharedStateRegistry();
}
