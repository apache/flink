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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;

/**
 * Gateway to send an {@link OperatorEvent} or {@link CoordinationRequest} from a Task to the {@link
 * OperatorCoordinator} JobManager side.
 *
 * <p>This is the first step in the chain of sending Operator Events and Requests from Operator to
 * Coordinator. Each layer adds further context, so that the inner layers do not need to know about
 * the complete context, which keeps dependencies small and makes testing easier.
 *
 * <pre>
 *     <li>{@code OperatorEventGateway} takes the event, enriches the event with the {@link OperatorID}, and
 *         forwards it to:</li>
 *     <li>{@link TaskOperatorEventGateway} enriches the event with the {@link ExecutionAttemptID} and
 *         forwards it to the:</li>
 *     <li>{@link JobMasterOperatorEventGateway} which is RPC interface from the TaskManager to the JobManager.</li>
 * </pre>
 */
public interface TaskOperatorEventGateway {

    /**
     * Sends an event from the operator (identified by the given operator ID) to the operator
     * coordinator (identified by the same ID).
     */
    void sendOperatorEventToCoordinator(OperatorID operator, SerializedValue<OperatorEvent> event);

    /**
     * Sends a request from current operator to a specified operator coordinator which is identified
     * by the given operator ID and return the response.
     */
    CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operator, SerializedValue<CoordinationRequest> request);
}
