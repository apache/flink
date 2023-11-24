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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;

/**
 * Message used in {@link org.apache.flink.runtime.jobmaster.JobMaster#sendRequestsToCoordinators}
 * to send coordination request to Coordinator on the JobManager side.
 */
public class CoordinationRequestMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final OperatorID operatorID;
    private final SerializedValue<CoordinationRequest> serializedRequest;

    public CoordinationRequestMessage(
            OperatorID operatorID, SerializedValue<CoordinationRequest> serializedRequest) {
        this.operatorID = operatorID;
        this.serializedRequest = serializedRequest;
    }

    public OperatorID getOperatorID() {
        return operatorID;
    }

    public SerializedValue<CoordinationRequest> getSerializedRequest() {
        return serializedRequest;
    }
}
