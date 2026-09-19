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

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;

/**
 * A {@link RecoveredInputChannel} that starts failing {@link #onRecoveredStateBuffer} once the
 * given number of buffers has been taken over. The failing call takes over its buffer as well, like
 * the production implementations do.
 */
public class FailingRecoveredInputChannel extends RecoveredInputChannel {

    private final int failAfterBuffers;

    private int deliveredBuffers;

    public FailingRecoveredInputChannel(SingleInputGate inputGate, int failAfterBuffers) {
        super(
                inputGate,
                0,
                new ResultPartitionID(),
                new ResultSubpartitionIndexSet(0),
                0,
                0,
                new SimpleCounter(),
                new SimpleCounter(),
                1);
        this.failAfterBuffers = failAfterBuffers;
    }

    @Override
    public void onRecoveredStateBuffer(Buffer buffer) {
        super.onRecoveredStateBuffer(buffer);
        if (++deliveredBuffers > failAfterBuffers) {
            throw new IllegalStateException("Delivery failed on purpose.");
        }
    }

    @Override
    protected InputChannel toInputChannelInternal(boolean needsRecovery) {
        return new TestInputChannel(inputGate, 0);
    }
}
