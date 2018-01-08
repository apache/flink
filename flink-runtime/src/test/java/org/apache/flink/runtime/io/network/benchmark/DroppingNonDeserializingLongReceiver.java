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

package org.apache.flink.runtime.io.network.benchmark;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

/**
 * {@link ReceiverThread} that drops all of the incoming messages.
 */
@SuppressWarnings("unused")
public class DroppingNonDeserializingLongReceiver extends ReceiverThread {

    private final InputGate inputGate;

    public DroppingNonDeserializingLongReceiver(InputGate inputGate, int expectedRepetitionsOfExpectedRecord) {
        super(expectedRepetitionsOfExpectedRecord);
        this.inputGate = inputGate;
    }

    protected void readRecords(long remaining) throws Exception {
        LOG.debug("readRecords(remaining = {})", remaining);
        // assume LongValue instances here (4 bytes length, 8 bytes long)
        final long expectedBytes = remaining * 12;

        long readBytes = 0;

        while (running && readBytes < expectedBytes) {
            BufferOrEvent input = inputGate.getNextBufferOrEvent();
            if (input.isBuffer()) {
                Buffer buffer = input.getBuffer();
                readBytes += buffer.getSize();
                buffer.recycle();
            }
        }

    }
}
