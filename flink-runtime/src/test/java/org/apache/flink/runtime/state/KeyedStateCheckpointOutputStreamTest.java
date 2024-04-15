/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyedStateCheckpointOutputStreamTest {

    private static final int STREAM_CAPACITY = 128;

    private static KeyedStateCheckpointOutputStream createStream(KeyGroupRange keyGroupRange) {
        CheckpointStateOutputStream checkStream =
                new TestMemoryCheckpointOutputStream(STREAM_CAPACITY);
        return new KeyedStateCheckpointOutputStream(checkStream, keyGroupRange);
    }

    private KeyGroupsStateHandle writeAllTestKeyGroups(
            KeyedStateCheckpointOutputStream stream, KeyGroupRange keyRange) throws Exception {

        DataOutputView dov = new DataOutputViewStreamWrapper(stream);
        for (int kg : keyRange) {
            stream.startNewKeyGroup(kg);
            dov.writeInt(kg);
        }

        return stream.closeAndGetHandle();
    }

    @Test
    void testCloseNotPropagated() throws Exception {
        KeyedStateCheckpointOutputStream stream = createStream(new KeyGroupRange(0, 0));
        TestMemoryCheckpointOutputStream innerStream =
                (TestMemoryCheckpointOutputStream) stream.getDelegate();
        stream.close();
        assertThat(innerStream.isClosed()).isFalse();
    }

    @Test
    void testEmptyKeyedStream() throws Exception {
        final KeyGroupRange keyRange = new KeyGroupRange(0, 2);
        KeyedStateCheckpointOutputStream stream = createStream(keyRange);
        TestMemoryCheckpointOutputStream innerStream =
                (TestMemoryCheckpointOutputStream) stream.getDelegate();
        KeyGroupsStateHandle emptyHandle = stream.closeAndGetHandle();
        assertThat(innerStream.isClosed()).isTrue();
        assertThat(emptyHandle).isNull();
    }

    @Test
    void testWriteReadRoundtrip() throws Exception {
        final KeyGroupRange keyRange = new KeyGroupRange(0, 2);
        KeyedStateCheckpointOutputStream stream = createStream(keyRange);
        KeyGroupsStateHandle fullHandle = writeAllTestKeyGroups(stream, keyRange);
        assertThat(fullHandle).isNotNull();

        verifyRead(fullHandle, keyRange);
    }

    @Test
    void testWriteKeyGroupTracking() throws Exception {
        final KeyGroupRange keyRange = new KeyGroupRange(0, 2);
        KeyedStateCheckpointOutputStream stream = createStream(keyRange);

        assertThatThrownBy(() -> stream.startNewKeyGroup(4711))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(stream.getCurrentKeyGroup()).isEqualTo(-1);

        DataOutputView dov = new DataOutputViewStreamWrapper(stream);
        int previous = -1;
        for (int kg : keyRange) {
            assertThat(stream.isKeyGroupAlreadyStarted(kg)).isFalse();
            assertThat(stream.isKeyGroupAlreadyFinished(kg)).isFalse();
            stream.startNewKeyGroup(kg);
            if (-1 != previous) {
                assertThat(stream.isKeyGroupAlreadyStarted(previous)).isTrue();
                assertThat(stream.isKeyGroupAlreadyFinished(previous)).isTrue();
            }
            assertThat(stream.isKeyGroupAlreadyStarted(kg)).isTrue();
            assertThat(stream.isKeyGroupAlreadyFinished(kg)).isFalse();
            dov.writeInt(kg);
            previous = kg;
        }

        KeyGroupsStateHandle fullHandle = stream.closeAndGetHandle();

        verifyRead(fullHandle, keyRange);

        for (int kg : keyRange) {
            assertThatThrownBy(() -> stream.startNewKeyGroup(kg)).isInstanceOf(IOException.class);
        }
    }

    @Test
    void testReadWriteMissingKeyGroups() throws Exception {
        final KeyGroupRange keyRange = new KeyGroupRange(0, 2);
        KeyedStateCheckpointOutputStream stream = createStream(keyRange);

        DataOutputView dov = new DataOutputViewStreamWrapper(stream);
        stream.startNewKeyGroup(1);
        dov.writeInt(1);

        KeyGroupsStateHandle fullHandle = stream.closeAndGetHandle();

        int count = 0;
        try (FSDataInputStream in = fullHandle.openInputStream()) {
            DataInputView div = new DataInputViewStreamWrapper(in);
            for (int kg : fullHandle.getKeyGroupRange()) {
                long off = fullHandle.getOffsetForKeyGroup(kg);
                if (off >= 0) {
                    in.seek(off);
                    assertThat(div.readInt()).isOne();
                    ++count;
                }
            }
        }

        assertThat(count).isOne();
    }

    private static void verifyRead(KeyGroupsStateHandle fullHandle, KeyGroupRange keyRange)
            throws IOException {
        int count = 0;
        try (FSDataInputStream in = fullHandle.openInputStream()) {
            DataInputView div = new DataInputViewStreamWrapper(in);
            for (int kg : fullHandle.getKeyGroupRange()) {
                long off = fullHandle.getOffsetForKeyGroup(kg);
                in.seek(off);
                assertThat(div.readInt()).isEqualTo(kg);
                ++count;
            }
        }

        assertThat(count).isEqualTo(keyRange.getNumberOfKeyGroups());
    }
}
