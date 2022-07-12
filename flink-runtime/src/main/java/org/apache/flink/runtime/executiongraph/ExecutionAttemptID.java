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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Unique identifier for the attempt to execute a tasks. Multiple attempts happen in cases of
 * failures and recovery.
 */
public class ExecutionAttemptID implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    // Represent the number of bytes occupied when writes ExecutionAttemptID to the ByteBuf.
    // It is the sum of one ExecutionGraphID type(executionGraphId), one ExecutionVertexID
    // type(executionVertexId) and one int type(attemptNumber).
    private static final int BYTE_BUF_LEN = ExecutionGraphID.SIZE + ExecutionVertexID.SIZE + 4;

    private final ExecutionGraphID executionGraphId;

    private final ExecutionVertexID executionVertexId;

    private final int attemptNumber;

    public ExecutionAttemptID(
            ExecutionGraphID executionGraphId,
            ExecutionVertexID executionVertexId,
            int attemptNumber) {
        checkArgument(attemptNumber >= 0);
        this.executionGraphId = checkNotNull(executionGraphId);
        this.executionVertexId = checkNotNull(executionVertexId);
        this.attemptNumber = attemptNumber;
    }

    public ExecutionVertexID getExecutionVertexId() {
        return executionVertexId;
    }

    public JobVertexID getJobVertexId() {
        return executionVertexId.getJobVertexId();
    }

    public int getSubtaskIndex() {
        return executionVertexId.getSubtaskIndex();
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public void writeTo(ByteBuf buf) {
        executionGraphId.writeTo(buf);
        executionVertexId.writeTo(buf);
        buf.writeInt(this.attemptNumber);
    }

    public static ExecutionAttemptID fromByteBuf(ByteBuf buf) {
        return new ExecutionAttemptID(
                ExecutionGraphID.fromByteBuf(buf),
                ExecutionVertexID.fromByteBuf(buf),
                buf.readInt());
    }

    public static int getByteBufLength() {
        return BYTE_BUF_LEN;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            ExecutionAttemptID that = (ExecutionAttemptID) obj;
            return that.executionGraphId.equals(this.executionGraphId)
                    && that.executionVertexId.equals(this.executionVertexId)
                    && that.attemptNumber == this.attemptNumber;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionGraphId, executionVertexId, attemptNumber);
    }

    @Override
    public String toString() {
        return String.format("%s_%s_%d", executionGraphId, executionVertexId, attemptNumber);
    }

    public static ExecutionAttemptID randomId() {
        return new ExecutionAttemptID(
                new ExecutionGraphID(), new ExecutionVertexID(new JobVertexID(0, 0), 0), 0);
    }
}
