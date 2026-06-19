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

package org.apache.flink.core.memory;

import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link MemorySegment} in on-heap mode. */
@ExtendWith(ParameterizedTestExtension.class)
class OnHeapMemorySegmentTest extends MemorySegmentTestBase {

    OnHeapMemorySegmentTest(int pageSize) {
        super(pageSize);
    }

    @Override
    MemorySegment createSegment(int size) {
        return MemorySegmentFactory.allocateUnpooledSegment(size);
    }

    @Override
    MemorySegment createSegment(int size, Object owner) {
        return MemorySegmentFactory.allocateUnpooledSegment(size, owner);
    }

    @TestTemplate
    void testHeapSegmentSpecifics() {
        final byte[] buffer = new byte[411];
        MemorySegment seg = new MemorySegment(buffer, null);

        assertThat(seg.isFreed()).isFalse();
        assertThat(seg.isOffHeap()).isFalse();
        assertThat(seg.size()).isEqualTo(buffer.length);
        assertThat(seg.getArray()).isSameAs(buffer);

        ByteBuffer buf1 = seg.wrap(1, 2);
        ByteBuffer buf2 = seg.wrap(3, 4);

        assertThat(buf2).isNotSameAs(buf1);
        assertThat(buf1.position()).isOne();
        assertThat(buf1.limit()).isEqualTo(3);
        assertThat(buf2.position()).isEqualTo(3);
        assertThat(buf2.limit()).isEqualTo(7);
    }

    @TestTemplate
    void testReadOnlyByteBufferPut() {
        final byte[] buffer = new byte[100];
        MemorySegment seg = new MemorySegment(buffer, null);

        String content = "hello world";
        ByteBuffer bb = ByteBuffer.allocate(20);
        bb.put(content.getBytes());
        bb.rewind();

        int offset = 10;
        int numBytes = 5;

        ByteBuffer readOnlyBuf = bb.asReadOnlyBuffer();
        assertThat(readOnlyBuf.isDirect()).isFalse();
        assertThat(readOnlyBuf.hasArray()).isFalse();

        seg.put(offset, readOnlyBuf, numBytes);

        // verify the area before the written region.
        for (int i = 0; i < offset; i++) {
            assertThat(buffer[i]).isZero();
        }

        // verify the region that is written.
        assertThat(new String(buffer, offset, numBytes)).isEqualTo("hello");

        // verify the area after the written region.
        for (int i = offset + numBytes; i < buffer.length; i++) {
            assertThat(buffer[i]).isZero();
        }
    }
}
