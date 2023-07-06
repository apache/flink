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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.NO_RESCALE;

/**
 * This class encapsulates state handles to the snapshots of all operator instances executed within
 * one task. A task can run multiple operator instances as a result of operator chaining, and all
 * operator instances from the chain can register their state under their operator id. Each operator
 * instance is a physical execution responsible for processing a partition of the data that goes
 * through a logical operator. This partitioning happens to parallelize execution of logical
 * operators, e.g. distributing a map function.
 *
 * <p>One instance of this class contains the information that one task will send to acknowledge a
 * checkpoint request by the checkpoint coordinator. Tasks run operator instances in parallel, so
 * the union of all {@link TaskStateSnapshot} that are collected by the checkpoint coordinator from
 * all tasks represent the whole state of a job at the time of the checkpoint.
 *
 * <p>This class should be called TaskState once the old class with this name that we keep for
 * backwards compatibility goes away.
 */
public class TaskStateSnapshot implements CompositeStateHandle {

    private static final long serialVersionUID = 1L;

    public static final TaskStateSnapshot FINISHED_ON_RESTORE =
            new TaskStateSnapshot(new HashMap<>(), true, true);

    /** Mapping from an operator id to the state of one subtask of this operator. */
    private final Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID;

    private final boolean isTaskDeployedAsFinished;

    private final boolean isTaskFinished;

    public TaskStateSnapshot() {
        this(10, false);
    }

    public TaskStateSnapshot(int size, boolean isTaskFinished) {
        this(CollectionUtil.newHashMapWithExpectedSize(size), false, isTaskFinished);
    }

    public TaskStateSnapshot(Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID) {
        this(subtaskStatesByOperatorID, false, false);
    }

    private TaskStateSnapshot(
            Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID,
            boolean isTaskDeployedAsFinished,
            boolean isTaskFinished) {
        this.subtaskStatesByOperatorID = Preconditions.checkNotNull(subtaskStatesByOperatorID);
        this.isTaskDeployedAsFinished = isTaskDeployedAsFinished;
        this.isTaskFinished = isTaskFinished;
    }

    /** Returns whether all the operators of the task are already finished on restoring. */
    public boolean isTaskDeployedAsFinished() {
        return isTaskDeployedAsFinished;
    }

    /** Returns whether all the operators of the task have called finished methods. */
    public boolean isTaskFinished() {
        return isTaskFinished;
    }

    /** Returns the subtask state for the given operator id (or null if not contained). */
    @Nullable
    public OperatorSubtaskState getSubtaskStateByOperatorID(OperatorID operatorID) {
        return subtaskStatesByOperatorID.get(operatorID);
    }

    /**
     * Maps the given operator id to the given subtask state. Returns the subtask state of a
     * previous mapping, if such a mapping existed or null otherwise.
     */
    public OperatorSubtaskState putSubtaskStateByOperatorID(
            @Nonnull OperatorID operatorID, @Nonnull OperatorSubtaskState state) {

        return subtaskStatesByOperatorID.put(operatorID, Preconditions.checkNotNull(state));
    }

    /** Returns the set of all mappings from operator id to the corresponding subtask state. */
    public Set<Map.Entry<OperatorID, OperatorSubtaskState>> getSubtaskStateMappings() {
        return subtaskStatesByOperatorID.entrySet();
    }

    /**
     * Returns true if at least one {@link OperatorSubtaskState} in subtaskStatesByOperatorID has
     * state.
     */
    public boolean hasState() {
        for (OperatorSubtaskState operatorSubtaskState : subtaskStatesByOperatorID.values()) {
            if (operatorSubtaskState != null && operatorSubtaskState.hasState()) {
                return true;
            }
        }
        return isTaskDeployedAsFinished;
    }

    /**
     * Returns the input channel mapping for rescaling with in-flight data or {@link
     * InflightDataRescalingDescriptor#NO_RESCALE}.
     */
    public InflightDataRescalingDescriptor getInputRescalingDescriptor() {
        return getMapping(OperatorSubtaskState::getInputRescalingDescriptor);
    }

    /**
     * Returns the output channel mapping for rescaling with in-flight data or {@link
     * InflightDataRescalingDescriptor#NO_RESCALE}.
     */
    public InflightDataRescalingDescriptor getOutputRescalingDescriptor() {
        return getMapping(OperatorSubtaskState::getOutputRescalingDescriptor);
    }

    @Override
    public void discardState() throws Exception {
        StateUtil.bestEffortDiscardAllStateObjects(subtaskStatesByOperatorID.values());
    }

    @Override
    public long getStateSize() {
        long size = 0L;

        for (OperatorSubtaskState subtaskState : subtaskStatesByOperatorID.values()) {
            if (subtaskState != null) {
                size += subtaskState.getStateSize();
            }
        }

        return size;
    }

    @Override
    public long getCheckpointedSize() {
        long size = 0L;

        for (OperatorSubtaskState subtaskState : subtaskStatesByOperatorID.values()) {
            if (subtaskState != null) {
                size += subtaskState.getCheckpointedSize();
            }
        }

        return size;
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        for (OperatorSubtaskState operatorSubtaskState : subtaskStatesByOperatorID.values()) {
            if (operatorSubtaskState != null) {
                operatorSubtaskState.registerSharedStates(stateRegistry, checkpointID);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TaskStateSnapshot that = (TaskStateSnapshot) o;

        return subtaskStatesByOperatorID.equals(that.subtaskStatesByOperatorID)
                && isTaskDeployedAsFinished == that.isTaskDeployedAsFinished
                && isTaskFinished == that.isTaskFinished;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtaskStatesByOperatorID, isTaskDeployedAsFinished, isTaskFinished);
    }

    @Override
    public String toString() {
        return "TaskOperatorSubtaskStates{"
                + "subtaskStatesByOperatorID="
                + subtaskStatesByOperatorID
                + ", isTaskDeployedAsFinished="
                + isTaskDeployedAsFinished
                + ", isTaskFinished="
                + isTaskFinished
                + '}';
    }

    /** Returns the only valid mapping as ensured by {@link StateAssignmentOperation}. */
    private InflightDataRescalingDescriptor getMapping(
            Function<OperatorSubtaskState, InflightDataRescalingDescriptor> mappingExtractor) {
        return Iterators.getOnlyElement(
                subtaskStatesByOperatorID.values().stream()
                        .map(mappingExtractor)
                        .filter(mapping -> !mapping.equals(NO_RESCALE))
                        .iterator(),
                NO_RESCALE);
    }

    @Nullable
    public static SerializedValue<TaskStateSnapshot> serializeTaskStateSnapshot(
            TaskStateSnapshot subtaskState) {
        try {
            return subtaskState == null ? null : new SerializedValue<>(subtaskState);
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Nullable
    public static TaskStateSnapshot deserializeTaskStateSnapshot(
            SerializedValue<TaskStateSnapshot> subtaskState, ClassLoader classLoader) {
        try {
            return subtaskState == null ? null : subtaskState.deserializeValue(classLoader);
        } catch (IOException | ClassNotFoundException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
