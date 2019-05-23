/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BinaryRowTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link SegmentsUtil}, most is covered by {@link BinaryRowTest},
 * this just test some boundary scenarios testing.
 */
public class SegmentsUtilTest {

	@Test
	public void testCopy() {
		// test copy the content of the latter Seg
		MemorySegment[] segments = new MemorySegment[2];
		segments[0] = MemorySegmentFactory.wrap(new byte[]{0, 2, 5});
		segments[1] = MemorySegmentFactory.wrap(new byte[]{6, 12, 15});

		byte[] bytes = SegmentsUtil.copyToBytes(segments, 4, 2);
		Assert.assertArrayEquals(new byte[] {12, 15}, bytes);
	}

	@Test
	public void testEquals() {
		// test copy the content of the latter Seg
		MemorySegment[] segments1 = new MemorySegment[3];
		segments1[0] = MemorySegmentFactory.wrap(new byte[]{0, 2, 5});
		segments1[1] = MemorySegmentFactory.wrap(new byte[]{6, 12, 15});
		segments1[2] = MemorySegmentFactory.wrap(new byte[]{1, 1, 1});

		MemorySegment[] segments2 = new MemorySegment[2];
		segments2[0] = MemorySegmentFactory.wrap(new byte[]{6, 0, 2, 5});
		segments2[1] = MemorySegmentFactory.wrap(new byte[]{6, 12, 15, 18});

		Assert.assertTrue(SegmentsUtil.equalsMultiSegments(segments1, 0, segments2, 0, 0));
		Assert.assertTrue(SegmentsUtil.equals(segments1, 0, segments2, 1, 3));
		Assert.assertTrue(SegmentsUtil.equals(segments1, 0, segments2, 1, 6));
		Assert.assertFalse(SegmentsUtil.equals(segments1, 0, segments2, 1, 7));
	}

}
