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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * Unique identifier for the attempt to execute a tasks. Multiple attempts happen in cases of
 * failures and recovery.
 */
public class ExecutionAttemptID implements java.io.Serializable {

    private static final long serialVersionUID = -1169683445778281344L;

    private final AbstractID executionAttemptId;

    public ExecutionAttemptID() {
        this(new AbstractID());
    }

    private ExecutionAttemptID(AbstractID id) {
        this.executionAttemptId = id;
    }

    @VisibleForTesting
    public ExecutionAttemptID(ExecutionAttemptID toCopy) {
        this.executionAttemptId = new AbstractID(toCopy.executionAttemptId);
    }

    public void writeTo(ByteBuf buf) {
        buf.writeLong(this.executionAttemptId.getLowerPart());
        buf.writeLong(this.executionAttemptId.getUpperPart());
    }

    public static ExecutionAttemptID fromByteBuf(ByteBuf buf) {
        return new ExecutionAttemptID(new AbstractID(buf.readLong(), buf.readLong()));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == getClass()) {
            ExecutionAttemptID that = (ExecutionAttemptID) obj;
            return that.executionAttemptId.equals(this.executionAttemptId);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return executionAttemptId.hashCode();
    }

    @Override
    public String toString() {
        return executionAttemptId.toString();
    }
}
