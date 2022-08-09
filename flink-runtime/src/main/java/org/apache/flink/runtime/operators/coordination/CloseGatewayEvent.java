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

package org.apache.flink.runtime.operators.coordination;

import java.util.Objects;

/**
 * An {@link OperatorEvent} sent from an {@link OperatorCoordinator} to its subtask to signal that
 * the communication gateway needs to be temporarily closed for a specific checkpoint.
 */
public class CloseGatewayEvent implements OperatorEvent {

    /** The ID of the checkpoint that this event is related to. */
    private final long checkpointId;

    /** The index of the subtask that this event is related to. */
    private final int subtaskIndex;

    public CloseGatewayEvent(long checkpointId, int subtaskIndex) {
        this.checkpointId = checkpointId;
        this.subtaskIndex = subtaskIndex;
    }

    long getCheckpointID() {
        return checkpointId;
    }

    int getSubtaskIndex() {
        return subtaskIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointId, subtaskIndex);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CloseGatewayEvent)) {
            return false;
        }
        CloseGatewayEvent event = (CloseGatewayEvent) obj;
        return event.checkpointId == this.checkpointId && event.subtaskIndex == this.subtaskIndex;
    }

    @Override
    public String toString() {
        return "CloseGatewayEvent (checkpointId: "
                + checkpointId
                + ", subtaskIndex: "
                + subtaskIndex
                + ')';
    }
}
