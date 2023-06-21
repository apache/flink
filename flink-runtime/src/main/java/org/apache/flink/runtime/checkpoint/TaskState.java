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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Simple container class which contains the task state and key-group state handles for the sub
 * tasks of a {@link org.apache.flink.runtime.jobgraph.JobVertex}.
 *
 * <p>This class basically groups all non-partitioned state and key-group state belonging to the
 * same job vertex together.
 *
 * @deprecated Internal class for savepoint backwards compatibility. Don't use for other purposes.
 */
@Deprecated
public class TaskState implements CompositeStateHandle {

    private static final long serialVersionUID = -4845578005863201810L;

    private final JobVertexID jobVertexID;

    /** handles to non-partitioned states, subtaskindex -> subtaskstate. */
    private final Map<Integer, SubtaskState> subtaskStates;

    /** parallelism of the operator when it was checkpointed. */
    private final int parallelism;

    /** maximum parallelism of the operator when the job was first created. */
    private final int maxParallelism;

    /** length of the operator chain. */
    private final int chainLength;

    public TaskState(
            JobVertexID jobVertexID, int parallelism, int maxParallelism, int chainLength) {
        Preconditions.checkArgument(
                parallelism <= maxParallelism,
                "Parallelism "
                        + parallelism
                        + " is not smaller or equal to max parallelism "
                        + maxParallelism
                        + ".");
        Preconditions.checkArgument(
                chainLength > 0, "There has to be at least one operator in the operator chain.");

        this.jobVertexID = jobVertexID;

        this.subtaskStates = CollectionUtil.newHashMapWithExpectedSize(parallelism);

        this.parallelism = parallelism;
        this.maxParallelism = maxParallelism;
        this.chainLength = chainLength;
    }

    public JobVertexID getJobVertexID() {
        return jobVertexID;
    }

    public void putState(int subtaskIndex, SubtaskState subtaskState) {
        Preconditions.checkNotNull(subtaskState);

        if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
            throw new IndexOutOfBoundsException(
                    "The given sub task index "
                            + subtaskIndex
                            + " exceeds the maximum number of sub tasks "
                            + subtaskStates.size());
        } else {
            subtaskStates.put(subtaskIndex, subtaskState);
        }
    }

    public SubtaskState getState(int subtaskIndex) {
        if (subtaskIndex < 0 || subtaskIndex >= parallelism) {
            throw new IndexOutOfBoundsException(
                    "The given sub task index "
                            + subtaskIndex
                            + " exceeds the maximum number of sub tasks "
                            + subtaskStates.size());
        } else {
            return subtaskStates.get(subtaskIndex);
        }
    }

    public Collection<SubtaskState> getStates() {
        return subtaskStates.values();
    }

    public int getNumberCollectedStates() {
        return subtaskStates.size();
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public int getChainLength() {
        return chainLength;
    }

    @Override
    public void discardState() throws Exception {
        for (SubtaskState subtaskState : subtaskStates.values()) {
            subtaskState.discardState();
        }
    }

    @Override
    public void registerSharedStates(SharedStateRegistry sharedStateRegistry, long checkpointID) {
        for (SubtaskState subtaskState : subtaskStates.values()) {
            subtaskState.registerSharedStates(sharedStateRegistry, checkpointID);
        }
    }

    @Override
    public long getCheckpointedSize() {
        // This class is actually deprecated, just return the state size.
        return getStateSize();
    }

    @Override
    public long getStateSize() {
        long result = 0L;

        for (int i = 0; i < parallelism; i++) {
            SubtaskState subtaskState = subtaskStates.get(i);
            if (subtaskState != null) {
                result += subtaskState.getStateSize();
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TaskState) {
            TaskState other = (TaskState) obj;

            return jobVertexID.equals(other.jobVertexID)
                    && parallelism == other.parallelism
                    && subtaskStates.equals(other.subtaskStates);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return parallelism + 31 * Objects.hash(jobVertexID, subtaskStates);
    }

    public Map<Integer, SubtaskState> getSubtaskStates() {
        return Collections.unmodifiableMap(subtaskStates);
    }

    @Override
    public String toString() {
        // KvStates are always null in 1.1. Don't print this as it might
        // confuse users that don't care about how we store it internally.
        return "TaskState("
                + "jobVertexID: "
                + jobVertexID
                + ", parallelism: "
                + parallelism
                + ", sub task states: "
                + subtaskStates.size()
                + ", total size (bytes): "
                + getStateSize()
                + ')';
    }
}
