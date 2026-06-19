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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OperatorStateOutputCheckpointStreamTest {

    private static final int STREAM_CAPACITY = 128;

    private static OperatorStateCheckpointOutputStream createStream() throws IOException {
        CheckpointStateOutputStream checkStream =
                new TestMemoryCheckpointOutputStream(STREAM_CAPACITY);
        return new OperatorStateCheckpointOutputStream(checkStream);
    }

    private OperatorStateHandle writeAllTestKeyGroups(
            OperatorStateCheckpointOutputStream stream, int numPartitions) throws Exception {

        DataOutputView dov = new DataOutputViewStreamWrapper(stream);
        for (int i = 0; i < numPartitions; ++i) {
            assertThat(stream.getNumberOfPartitions()).isEqualTo(i);
            stream.startNewPartition();
            dov.writeInt(i);
        }

        return stream.closeAndGetHandle();
    }

    @Test
    void testCloseNotPropagated() throws Exception {
        OperatorStateCheckpointOutputStream stream = createStream();
        TestMemoryCheckpointOutputStream innerStream =
                (TestMemoryCheckpointOutputStream) stream.getDelegate();
        stream.close();
        assertThat(innerStream.isClosed()).isFalse();
        innerStream.close();
    }

    @Test
    void testEmptyOperatorStream() throws Exception {
        OperatorStateCheckpointOutputStream stream = createStream();
        TestMemoryCheckpointOutputStream innerStream =
                (TestMemoryCheckpointOutputStream) stream.getDelegate();
        OperatorStateHandle emptyHandle = stream.closeAndGetHandle();
        assertThat(innerStream.isClosed()).isTrue();
        assertThat(stream.getNumberOfPartitions()).isZero();
        assertThat(emptyHandle).isNull();
    }

    @Test
    void testWriteReadRoundtrip() throws Exception {
        int numPartitions = 3;
        OperatorStateCheckpointOutputStream stream = createStream();
        OperatorStateHandle fullHandle = writeAllTestKeyGroups(stream, numPartitions);
        assertThat(fullHandle).isNotNull();

        Map<String, OperatorStateHandle.StateMetaInfo> stateNameToPartitionOffsets =
                fullHandle.getStateNameToPartitionOffsets();
        for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry :
                stateNameToPartitionOffsets.entrySet()) {

            assertThat(entry.getValue().getDistributionMode())
                    .isEqualTo(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
        }
        verifyRead(fullHandle, numPartitions);
    }

    private static void verifyRead(OperatorStateHandle fullHandle, int numPartitions)
            throws IOException {
        int count = 0;
        try (FSDataInputStream in = fullHandle.openInputStream()) {
            OperatorStateHandle.StateMetaInfo metaInfo =
                    fullHandle
                            .getStateNameToPartitionOffsets()
                            .get(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);

            long[] offsets = metaInfo.getOffsets();

            assertThat(offsets).isNotNull();

            DataInputView div = new DataInputViewStreamWrapper(in);
            for (int i = 0; i < numPartitions; ++i) {
                in.seek(offsets[i]);
                assertThat(div.readInt()).isEqualTo(i);
                ++count;
            }
        }

        assertThat(count).isEqualTo(numPartitions);
    }
}
