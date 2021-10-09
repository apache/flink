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

package org.apache.flink.table.data.binary;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.util.DataFormatTestUtil;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.table.data.binary.BinaryRowDataUtil.BYTE_ARRAY_BASE_OFFSET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link BinarySegmentUtils}, most is covered by {@link
 * org.apache.flink.table.data.BinaryRowDataTest}, this just test some boundary scenarios testing.
 */
public class BinarySegmentUtilsTest {

    @Test
    public void testCopy() {
        // test copy the content of the latter Seg
        MemorySegment[] segments = new MemorySegment[2];
        segments[0] = MemorySegmentFactory.wrap(new byte[] {0, 2, 5});
        segments[1] = MemorySegmentFactory.wrap(new byte[] {6, 12, 15});

        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, 4, 2);
        Assert.assertArrayEquals(new byte[] {12, 15}, bytes);
    }

    @Test
    public void testEquals() {
        // test copy the content of the latter Seg
        MemorySegment[] segments1 = new MemorySegment[3];
        segments1[0] = MemorySegmentFactory.wrap(new byte[] {0, 2, 5});
        segments1[1] = MemorySegmentFactory.wrap(new byte[] {6, 12, 15});
        segments1[2] = MemorySegmentFactory.wrap(new byte[] {1, 1, 1});

        MemorySegment[] segments2 = new MemorySegment[2];
        segments2[0] = MemorySegmentFactory.wrap(new byte[] {6, 0, 2, 5});
        segments2[1] = MemorySegmentFactory.wrap(new byte[] {6, 12, 15, 18});

        assertTrue(BinarySegmentUtils.equalsMultiSegments(segments1, 0, segments2, 0, 0));
        assertTrue(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 3));
        assertTrue(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 6));
        assertFalse(BinarySegmentUtils.equals(segments1, 0, segments2, 1, 7));
    }

    @Test
    public void testBoundaryByteArrayEquals() {
        byte[] bytes1 = new byte[5];
        bytes1[3] = 81;
        byte[] bytes2 = new byte[100];
        bytes2[3] = 81;
        bytes2[4] = 81;

        assertTrue(BinaryRowDataUtil.byteArrayEquals(bytes1, bytes2, 4));
        assertFalse(BinaryRowDataUtil.byteArrayEquals(bytes1, bytes2, 5));
        assertTrue(BinaryRowDataUtil.byteArrayEquals(bytes1, bytes2, 0));
    }

    @Test
    public void testBoundaryEquals() {
        BinaryRowData row24 = DataFormatTestUtil.get24BytesBinaryRow();
        BinaryRowData row160 = DataFormatTestUtil.get160BytesBinaryRow();
        BinaryRowData varRow160 = DataFormatTestUtil.getMultiSeg160BytesBinaryRow(row160);
        BinaryRowData varRow160InOne = DataFormatTestUtil.getMultiSeg160BytesInOneSegRow(row160);

        assertEquals(row160, varRow160InOne);
        assertEquals(varRow160, varRow160InOne);
        assertEquals(row160, varRow160);
        assertEquals(varRow160InOne, varRow160);

        assertNotEquals(row24, row160);
        assertNotEquals(row24, varRow160);
        assertNotEquals(row24, varRow160InOne);

        assertTrue(BinarySegmentUtils.equals(row24.getSegments(), 0, row160.getSegments(), 0, 0));
        assertTrue(
                BinarySegmentUtils.equals(row24.getSegments(), 0, varRow160.getSegments(), 0, 0));

        // test var segs
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegmentFactory.wrap(new byte[16]);
        segments2[1] = MemorySegmentFactory.wrap(new byte[16]);
        segments2[2] = MemorySegmentFactory.wrap(new byte[16]);

        segments1[0].put(9, (byte) 1);
        assertFalse(BinarySegmentUtils.equals(segments1, 0, segments2, 14, 14));
        segments2[1].put(7, (byte) 1);
        assertTrue(BinarySegmentUtils.equals(segments1, 0, segments2, 14, 14));
        assertTrue(BinarySegmentUtils.equals(segments1, 2, segments2, 16, 14));
        assertTrue(BinarySegmentUtils.equals(segments1, 2, segments2, 16, 16));

        segments2[2].put(7, (byte) 1);
        assertTrue(BinarySegmentUtils.equals(segments1, 2, segments2, 32, 14));
    }

    @Test
    public void testBoundaryCopy() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[0].put(15, (byte) 5);
        segments1[1].put(15, (byte) 6);

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 0, bytes, 0, 64);
            assertTrue(BinarySegmentUtils.equals(segments1, 0, segments2, 0, 64));
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 32, bytes, 0, 14);
            assertTrue(BinarySegmentUtils.equals(segments1, 32, segments2, 0, 14));
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToBytes(segments1, 34, bytes, 0, 14);
            assertTrue(BinarySegmentUtils.equals(segments1, 34, segments2, 0, 14));
        }
    }

    @Test
    public void testCopyToUnsafe() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[0].put(15, (byte) 5);
        segments1[1].put(15, (byte) 6);

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToUnsafe(segments1, 0, bytes, BYTE_ARRAY_BASE_OFFSET, 64);
            assertTrue(BinarySegmentUtils.equals(segments1, 0, segments2, 0, 64));
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToUnsafe(segments1, 32, bytes, BYTE_ARRAY_BASE_OFFSET, 14);
            assertTrue(BinarySegmentUtils.equals(segments1, 32, segments2, 0, 14));
        }

        {
            byte[] bytes = new byte[64];
            MemorySegment[] segments2 = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};

            BinarySegmentUtils.copyToUnsafe(segments1, 34, bytes, BYTE_ARRAY_BASE_OFFSET, 14);
            assertTrue(BinarySegmentUtils.equals(segments1, 34, segments2, 0, 14));
        }
    }

    @Test
    public void testFind() {
        MemorySegment[] segments1 = new MemorySegment[2];
        segments1[0] = MemorySegmentFactory.wrap(new byte[32]);
        segments1[1] = MemorySegmentFactory.wrap(new byte[32]);
        MemorySegment[] segments2 = new MemorySegment[3];
        segments2[0] = MemorySegmentFactory.wrap(new byte[16]);
        segments2[1] = MemorySegmentFactory.wrap(new byte[16]);
        segments2[2] = MemorySegmentFactory.wrap(new byte[16]);

        assertEquals(34, BinarySegmentUtils.find(segments1, 34, 0, segments2, 0, 0));
        assertEquals(-1, BinarySegmentUtils.find(segments1, 34, 0, segments2, 0, 15));
    }
}
