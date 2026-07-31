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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteRecoveredInputChannelTest {

    @Test
    void testToInputChannelRequiresEmptyRecoveredBuffers() throws Exception {
        SingleInputGate inputGate = new SingleInputGateBuilder().build();
        RemoteRecoveredInputChannel recoveredChannel =
                InputChannelBuilder.newBuilder()
                        .setStateWriter(ChannelStateWriter.NO_OP)
                        .buildRemoteRecoveredChannel(inputGate);

        Buffer buffer = TestBufferFactory.createBuffer(13);
        recoveredChannel.onRecoveredStateBuffer(buffer);

        try {
            recoveredChannel.finishReadRecoveredState();
            assertThatThrownBy(() -> recoveredChannel.toInputChannel(false))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Received buffer should be empty");
        } finally {
            recoveredChannel.releaseAllResources();
        }
    }
}
