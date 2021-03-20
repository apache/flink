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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link MemorySegment} in off-heap mode using direct memory. */
@RunWith(Parameterized.class)
public class OffHeapDirectMemorySegmentTest extends MemorySegmentTestBase {

    public OffHeapDirectMemorySegmentTest(int pageSize) {
        super(pageSize);
    }

    @Override
    MemorySegment createSegment(int size) {
        return MemorySegmentFactory.allocateUnpooledOffHeapMemory(size);
    }

    @Override
    MemorySegment createSegment(int size, Object owner) {
        return MemorySegmentFactory.allocateUnpooledOffHeapMemory(size, owner);
    }

    @Test
    public void testHeapSegmentSpecifics() {
        final int bufSize = 411;
        MemorySegment seg = createSegment(bufSize);

        assertFalse(seg.isFreed());
        assertTrue(seg.isOffHeap());
        assertEquals(bufSize, seg.size());

        try {
            //noinspection ResultOfMethodCallIgnored
            seg.getArray();
            fail("should throw an exception");
        } catch (IllegalStateException e) {
            // expected
        }

        ByteBuffer buf1 = seg.wrap(1, 2);
        ByteBuffer buf2 = seg.wrap(3, 4);

        assertNotSame(buf1, buf2);
        assertEquals(1, buf1.position());
        assertEquals(3, buf1.limit());
        assertEquals(3, buf2.position());
        assertEquals(7, buf2.limit());
    }
}
