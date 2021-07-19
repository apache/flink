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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateObjectID;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/** This class encapsulates the data from the job manager to restore a task. */
public class JobManagerTaskRestore implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The id of the checkpoint from which we restore. */
    private final long restoreCheckpointId;

    /** The state for this task to restore. */
    private final TaskStateSnapshot taskStateSnapshot;

    /**
     * {@link StateObjectID IDs} of {@link StateObject state objects} in the {@link
     * #taskStateSnapshot snapshot} that this task shares with some other tasks. Those tasks can be
     * running in the same or in a different TM.
     *
     * <p>Updated only during recovery by {@link StateAssignmentOperation} on JM.
     *
     * <p>The task can read these objects but must NOT discard them. It notifies JM whether it still
     * uses those objects or not by including {@link StateObject}s (not IDs) into the snapshot when
     * acknowledging a checkpoint.
     */
    private final HashSet<StateObjectID> sharedStateObjectIDs = new HashSet<>();

    public JobManagerTaskRestore(
            @Nonnegative long restoreCheckpointId, @Nonnull TaskStateSnapshot taskStateSnapshot) {
        this.restoreCheckpointId = restoreCheckpointId;
        this.taskStateSnapshot = taskStateSnapshot;
    }

    public long getRestoreCheckpointId() {
        return restoreCheckpointId;
    }

    @Nonnull
    public TaskStateSnapshot getTaskStateSnapshot() {
        return taskStateSnapshot;
    }

    @Override
    public String toString() {
        return "JobManagerTaskRestore{"
                + "restoreCheckpointId="
                + restoreCheckpointId
                + ", taskStateSnapshot="
                + taskStateSnapshot
                + '}';
    }

    public Set<StateObjectID> getSharedStateObjectIDs() {
        return unmodifiableSet(sharedStateObjectIDs);
    }

    void addSharedObjectStateID(StateObjectID sharedStateID) {
        sharedStateObjectIDs.add(sharedStateID);
    }
}
