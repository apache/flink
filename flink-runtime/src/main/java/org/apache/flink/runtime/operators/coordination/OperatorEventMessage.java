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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;

/**
 * Message used in {@link
 * org.apache.flink.runtime.jobmaster.JobMaster#sendOperatorEventsToCoordinators} to send operator
 * event to Coordinator on the JobManager side.
 */
public final class OperatorEventMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ExecutionAttemptID task;
    private final OperatorID operatorID;
    private final SerializedValue<OperatorEvent> serializedEvent;

    public OperatorEventMessage(
            ExecutionAttemptID task,
            OperatorID operatorID,
            SerializedValue<OperatorEvent> serializedEvent) {
        this.operatorID = operatorID;
        this.task = task;
        this.serializedEvent = serializedEvent;
    }

    public ExecutionAttemptID getTask() {
        return task;
    }

    public OperatorID getOperatorID() {
        return operatorID;
    }

    public SerializedValue<OperatorEvent> getSerializedEvent() {
        return serializedEvent;
    }
}
