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

import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Container for the chained state of one parallel subtask of an operator/task. This is part of the
 * {@link TaskState}.
 */
public class SubtaskState implements CompositeStateHandle {

    private static final Logger LOG = LoggerFactory.getLogger(SubtaskState.class);

    private static final long serialVersionUID = -2394696997971923995L;

    /** Snapshot from the {@link org.apache.flink.runtime.state.OperatorStateBackend}. */
    private final ChainedStateHandle<OperatorStateHandle> managedOperatorState;

    /**
     * Snapshot written using {@link
     * org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
     */
    private final ChainedStateHandle<OperatorStateHandle> rawOperatorState;

    /** Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}. */
    private final KeyedStateHandle managedKeyedState;

    /**
     * Snapshot written using {@link
     * org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
     */
    private final KeyedStateHandle rawKeyedState;

    /**
     * The state size. This is also part of the deserialized state handle. We store it here in order
     * to not deserialize the state handle when gathering stats.
     */
    private final long stateSize;

    public SubtaskState(
            ChainedStateHandle<OperatorStateHandle> managedOperatorState,
            ChainedStateHandle<OperatorStateHandle> rawOperatorState,
            KeyedStateHandle managedKeyedState,
            KeyedStateHandle rawKeyedState) {

        this.managedOperatorState = managedOperatorState;
        this.rawOperatorState = rawOperatorState;
        this.managedKeyedState = managedKeyedState;
        this.rawKeyedState = rawKeyedState;

        try {
            long calculateStateSize = getSizeNullSafe(managedOperatorState);
            calculateStateSize += getSizeNullSafe(rawOperatorState);
            calculateStateSize += getSizeNullSafe(managedKeyedState);
            calculateStateSize += getSizeNullSafe(rawKeyedState);
            stateSize = calculateStateSize;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get state size.", e);
        }
    }

    private static long getSizeNullSafe(StateObject stateObject) throws Exception {
        return stateObject != null ? stateObject.getStateSize() : 0L;
    }

    // --------------------------------------------------------------------------------------------

    public ChainedStateHandle<OperatorStateHandle> getManagedOperatorState() {
        return managedOperatorState;
    }

    public ChainedStateHandle<OperatorStateHandle> getRawOperatorState() {
        return rawOperatorState;
    }

    public KeyedStateHandle getManagedKeyedState() {
        return managedKeyedState;
    }

    public KeyedStateHandle getRawKeyedState() {
        return rawKeyedState;
    }

    @Override
    public void discardState() {
        try {
            StateUtil.bestEffortDiscardAllStateObjects(
                    Arrays.asList(
                            managedOperatorState,
                            rawOperatorState,
                            managedKeyedState,
                            rawKeyedState));
        } catch (Exception e) {
            LOG.warn("Error while discarding operator states.", e);
        }
    }

    @Override
    public void registerSharedStates(SharedStateRegistry sharedStateRegistry) {
        if (managedKeyedState != null) {
            managedKeyedState.registerSharedStates(sharedStateRegistry);
        }

        if (rawKeyedState != null) {
            rawKeyedState.registerSharedStates(sharedStateRegistry);
        }
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubtaskState that = (SubtaskState) o;

        if (stateSize != that.stateSize) {
            return false;
        }

        if (managedOperatorState != null
                ? !managedOperatorState.equals(that.managedOperatorState)
                : that.managedOperatorState != null) {
            return false;
        }
        if (rawOperatorState != null
                ? !rawOperatorState.equals(that.rawOperatorState)
                : that.rawOperatorState != null) {
            return false;
        }
        if (managedKeyedState != null
                ? !managedKeyedState.equals(that.managedKeyedState)
                : that.managedKeyedState != null) {
            return false;
        }
        return rawKeyedState != null
                ? rawKeyedState.equals(that.rawKeyedState)
                : that.rawKeyedState == null;
    }

    @Override
    public int hashCode() {
        int result = (managedOperatorState != null ? managedOperatorState.hashCode() : 0);
        result = 31 * result + (rawOperatorState != null ? rawOperatorState.hashCode() : 0);
        result = 31 * result + (managedKeyedState != null ? managedKeyedState.hashCode() : 0);
        result = 31 * result + (rawKeyedState != null ? rawKeyedState.hashCode() : 0);
        result = 31 * result + (int) (stateSize ^ (stateSize >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "SubtaskState{"
                + "operatorStateFromBackend="
                + managedOperatorState
                + ", operatorStateFromStream="
                + rawOperatorState
                + ", keyedStateFromBackend="
                + managedKeyedState
                + ", keyedStateFromStream="
                + rawKeyedState
                + ", stateSize="
                + stateSize
                + '}';
    }
}
