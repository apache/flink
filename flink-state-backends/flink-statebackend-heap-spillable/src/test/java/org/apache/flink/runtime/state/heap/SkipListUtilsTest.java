/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.state.heap.SkipListUtils.MAX_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SkipListUtils}. */
class SkipListUtilsTest {

    @Test
    void testKeySpacePutAndGet() {
        for (int level = 0; level <= MAX_LEVEL; level++) {
            int keyLen = ThreadLocalRandom.current().nextInt(100) + 1;
            KeySpace keySpace = createKeySpace(level, keyLen);
            int keyMetaLen = SkipListUtils.getKeyMetaLen(level);
            int totalKeySpaceLen = keyMetaLen + keyLen;
            int offset = 100;
            MemorySegment segment =
                    MemorySegmentFactory.allocateUnpooledSegment(totalKeySpaceLen + offset);
            putKeySpace(keySpace, segment, offset);
            verifyGetKeySpace(keySpace, segment, offset);
        }
    }

    @Test
    void testValueSpacePutAndGet() {
        for (int i = 0; i < 100; i++) {
            int valueLen = ThreadLocalRandom.current().nextInt(100) + 1;
            ValueSpace valueSpace = createValueSpace(valueLen);
            int valueMetaLen = SkipListUtils.getValueMetaLen();
            int totalValueSpaceLen = valueMetaLen + valueLen;
            int offset = 100;
            MemorySegment segment =
                    MemorySegmentFactory.allocateUnpooledSegment(totalValueSpaceLen + offset);
            putValueSpace(valueSpace, segment, offset);
            verifyGetValueSpace(valueSpace, segment, offset);
        }
    }

    private KeySpace createKeySpace(int level, int keyLen) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        KeySpace keySpace = new KeySpace();
        keySpace.level = level;
        keySpace.status = random.nextBoolean() ? NodeStatus.PUT : NodeStatus.REMOVE;
        keySpace.valuePointer = random.nextLong();
        keySpace.nextKeyPointer = random.nextLong();
        keySpace.nextIndexNodes = new long[level];
        keySpace.prevIndexNodes = new long[level];
        for (int i = 0; i < level; i++) {
            keySpace.nextIndexNodes[i] = random.nextLong();
            keySpace.prevIndexNodes[i] = random.nextLong();
        }
        keySpace.keyData = new byte[keyLen];
        random.nextBytes(keySpace.keyData);
        return keySpace;
    }

    private void putKeySpace(KeySpace keySpace, MemorySegment memorySegment, int offset) {
        SkipListUtils.putLevelAndNodeStatus(memorySegment, offset, keySpace.level, keySpace.status);
        SkipListUtils.putKeyLen(memorySegment, offset, keySpace.keyData.length);
        SkipListUtils.putValuePointer(memorySegment, offset, keySpace.valuePointer);
        SkipListUtils.putNextKeyPointer(memorySegment, offset, keySpace.nextKeyPointer);
        for (int i = 1; i <= keySpace.nextIndexNodes.length; i++) {
            SkipListUtils.putNextIndexNode(
                    memorySegment, offset, i, keySpace.nextIndexNodes[i - 1]);
        }
        for (int i = 1; i <= keySpace.prevIndexNodes.length; i++) {
            SkipListUtils.putPrevIndexNode(
                    memorySegment, offset, keySpace.level, i, keySpace.prevIndexNodes[i - 1]);
        }
        SkipListUtils.putKeyData(
                memorySegment,
                offset,
                MemorySegmentFactory.wrap(keySpace.keyData),
                0,
                keySpace.keyData.length,
                keySpace.level);
    }

    private void verifyGetKeySpace(KeySpace keySpace, MemorySegment memorySegment, int offset) {
        assertThat(SkipListUtils.getLevel(memorySegment, offset)).isEqualTo(keySpace.level);
        assertThat(SkipListUtils.getNodeStatus(memorySegment, offset)).isEqualTo(keySpace.status);
        assertThat(SkipListUtils.getKeyLen(memorySegment, offset))
                .isEqualTo(keySpace.keyData.length);
        assertThat(SkipListUtils.getValuePointer(memorySegment, offset))
                .isEqualTo(keySpace.valuePointer);
        assertThat(SkipListUtils.getNextKeyPointer(memorySegment, offset))
                .isEqualTo(keySpace.nextKeyPointer);
        for (int i = 1; i <= keySpace.nextIndexNodes.length; i++) {
            assertThat(SkipListUtils.getNextIndexNode(memorySegment, offset, i))
                    .isEqualTo(keySpace.nextIndexNodes[i - 1]);
        }
        for (int i = 1; i <= keySpace.prevIndexNodes.length; i++) {
            assertThat(SkipListUtils.getPrevIndexNode(memorySegment, offset, keySpace.level, i))
                    .isEqualTo(keySpace.prevIndexNodes[i - 1]);
        }
        int keyDataOffset = SkipListUtils.getKeyDataOffset(keySpace.level);
        MemorySegment keyDataSegment = MemorySegmentFactory.wrap(keySpace.keyData);
        assertThat(
                        memorySegment.compare(
                                keyDataSegment, offset + keyDataOffset, 0, keySpace.keyData.length))
                .isEqualTo(0);
    }

    private ValueSpace createValueSpace(int valueLen) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        ValueSpace valueSpace = new ValueSpace();
        valueSpace.version = random.nextInt(Integer.MAX_VALUE);
        valueSpace.keyPointer = random.nextLong();
        valueSpace.nextValuePointer = random.nextLong();
        valueSpace.valueData = new byte[valueLen];
        random.nextBytes(valueSpace.valueData);
        return valueSpace;
    }

    private void putValueSpace(ValueSpace valueSpace, MemorySegment memorySegment, int offset) {
        SkipListUtils.putValueVersion(memorySegment, offset, valueSpace.version);
        SkipListUtils.putKeyPointer(memorySegment, offset, valueSpace.keyPointer);
        SkipListUtils.putNextValuePointer(memorySegment, offset, valueSpace.nextValuePointer);
        SkipListUtils.putValueLen(memorySegment, offset, valueSpace.valueData.length);
        SkipListUtils.putValueData(memorySegment, offset, valueSpace.valueData);
    }

    private void verifyGetValueSpace(
            ValueSpace valueSpace, MemorySegment memorySegment, int offset) {
        assertThat(SkipListUtils.getValueVersion(memorySegment, offset))
                .isEqualTo(valueSpace.version);
        assertThat(SkipListUtils.getKeyPointer(memorySegment, offset))
                .isEqualTo(valueSpace.keyPointer);
        assertThat(SkipListUtils.getNextValuePointer(memorySegment, offset))
                .isEqualTo(valueSpace.nextValuePointer);
        assertThat(SkipListUtils.getValueLen(memorySegment, offset))
                .isEqualTo(valueSpace.valueData.length);
        int valueDataOffset = SkipListUtils.getValueMetaLen();
        MemorySegment valueDataSegment = MemorySegmentFactory.wrap(valueSpace.valueData);
        assertThat(
                        memorySegment.compare(
                                valueDataSegment,
                                offset + valueDataOffset,
                                0,
                                valueSpace.valueData.length))
                .isEqualTo(0);
    }

    /** Used to test key space. */
    static class KeySpace {
        int level;
        NodeStatus status;
        long valuePointer;
        long nextKeyPointer;
        long[] nextIndexNodes;
        long[] prevIndexNodes;
        byte[] keyData;
    }

    /** Used to test value space. */
    static class ValueSpace {
        int version;
        long keyPointer;
        long nextValuePointer;
        byte[] valueData;
    }
}
