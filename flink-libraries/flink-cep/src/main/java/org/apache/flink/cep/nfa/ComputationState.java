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

package org.apache.flink.cep.nfa;

import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Helper class which encapsulates the currentStateName of the NFA computation. It points to the
 * current currentStateName, the previous entry of the pattern, the current version and the starting
 * timestamp of the overall pattern.
 */
public class ComputationState {
    // pointer to the NFA currentStateName of the computation
    private final String currentStateName;

    // The current version of the currentStateName to discriminate the valid pattern paths in the
    // SharedBuffer
    private final DeweyNumber version;

    // Timestamp of the first element in the pattern
    private final long startTimestamp;

    @Nullable private final NodeId previousBufferEntry;

    @Nullable private final EventId startEventID;

    private ComputationState(
            final String currentState,
            @Nullable final NodeId previousBufferEntry,
            final DeweyNumber version,
            @Nullable final EventId startEventID,
            final long startTimestamp) {
        this.currentStateName = currentState;
        this.version = version;
        this.startTimestamp = startTimestamp;
        this.previousBufferEntry = previousBufferEntry;
        this.startEventID = startEventID;
    }

    public EventId getStartEventID() {
        return startEventID;
    }

    public NodeId getPreviousBufferEntry() {
        return previousBufferEntry;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public String getCurrentStateName() {
        return currentStateName;
    }

    public DeweyNumber getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ComputationState) {
            ComputationState other = (ComputationState) obj;
            return Objects.equals(currentStateName, other.currentStateName)
                    && Objects.equals(version, other.version)
                    && startTimestamp == other.startTimestamp
                    && Objects.equals(startEventID, other.startEventID)
                    && Objects.equals(previousBufferEntry, other.previousBufferEntry);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "ComputationState{"
                + "currentStateName='"
                + currentStateName
                + '\''
                + ", version="
                + version
                + ", startTimestamp="
                + startTimestamp
                + ", previousBufferEntry="
                + previousBufferEntry
                + ", startEventID="
                + startEventID
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                currentStateName, version, startTimestamp, startEventID, previousBufferEntry);
    }

    public static ComputationState createStartState(final String state) {
        return createStartState(state, new DeweyNumber(1));
    }

    public static ComputationState createStartState(final String state, final DeweyNumber version) {
        return createState(state, null, version, -1L, null);
    }

    public static ComputationState createState(
            final String currentState,
            final NodeId previousEntry,
            final DeweyNumber version,
            final long startTimestamp,
            final EventId startEventID) {
        return new ComputationState(
                currentState, previousEntry, version, startEventID, startTimestamp);
    }
}
