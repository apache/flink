/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecoveryCheckpointBarrier}. */
class RecoveryCheckpointBarrierTest {

    @Test
    void testReflectiveWriteIsUnsupported() {
        RecoveryCheckpointBarrier barrier = new RecoveryCheckpointBarrier(1L);

        assertThatThrownBy(() -> barrier.write(new DataOutputSerializer(Long.BYTES)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("dedicated type-tag path");
    }

    @Test
    void testEventSerializerHandlesRecoveryCheckpointBarrier() throws Exception {
        long checkpointId = 123L;

        Buffer buffer =
                EventSerializer.toBuffer(new RecoveryCheckpointBarrier(checkpointId), false);
        Object deserialized =
                EventSerializer.fromBuffer(
                        buffer, RecoveryCheckpointBarrier.class.getClassLoader());

        assertThat(deserialized).isEqualTo(new RecoveryCheckpointBarrier(checkpointId));
    }
}
