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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.io.Serializable;

/**
 * Runtime identifier of a consumed {@link
 * org.apache.flink.runtime.executiongraph.IntermediateResult}.
 *
 * <p>At runtime the {@link org.apache.flink.runtime.jobgraph.IntermediateDataSetID} is not enough
 * to uniquely identify an input gate. It needs to be associated with the consuming task as well to
 * ensure correct tracking of gates in shuffle implementation.
 */
public class InputGateID implements Serializable {

    private static final long serialVersionUID = 4613970383536333315L;

    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    private final IntermediateDataSetID consumedResultID;

    /**
     * The ID of the consumer.
     *
     * <p>The ID of {@link org.apache.flink.runtime.executiongraph.Execution} and its local {@link
     * org.apache.flink.runtime.taskmanager.Task}.
     */
    private final ExecutionAttemptID consumerID;

    public InputGateID(IntermediateDataSetID consumedResultID, ExecutionAttemptID consumerID) {
        this.consumedResultID = consumedResultID;
        this.consumerID = consumerID;
    }

    public IntermediateDataSetID getConsumedResultID() {
        return consumedResultID;
    }

    public ExecutionAttemptID getConsumerID() {
        return consumerID;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj.getClass() == InputGateID.class) {
            InputGateID o = (InputGateID) obj;

            return o.getConsumedResultID().equals(consumedResultID)
                    && o.getConsumerID().equals(consumerID);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return consumedResultID.hashCode() ^ consumerID.hashCode();
    }

    @Override
    public String toString() {
        return consumedResultID.toString() + "@" + consumerID.toString();
    }
}
