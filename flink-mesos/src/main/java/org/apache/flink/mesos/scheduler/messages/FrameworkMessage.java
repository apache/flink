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

package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Message sent when an executor sends a message. These messages are best effort; do not expect a
 * framework message to be retransmitted in any reliable fashion.
 */
public class FrameworkMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Protos.ExecutorID executorId;

    private final Protos.SlaveID slaveId;

    private final byte[] data;

    public FrameworkMessage(Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        this.executorId = executorId;
        this.slaveId = slaveId;
        this.data = data;
    }

    public Protos.ExecutorID executorId() {
        return executorId;
    }

    public Protos.SlaveID slaveId() {
        return slaveId;
    }

    public byte[] data() {
        return data;
    }

    @Override
    public String toString() {
        return "FrameworkMessage{"
                + "executorId="
                + executorId
                + ", slaveId="
                + slaveId
                + ", data="
                + Arrays.toString(data)
                + '}';
    }
}
