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

package org.apache.flink.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link LongValueSequenceIterator}.
 */
public class LongValueSequenceIteratorTest extends TestLogger {

	@Test
	public void testSplitRegular() {
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(0, 10), 2);
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(100, 100000), 7);
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-100, 0), 5);
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-100, 100), 3);
	}

	@Test
	public void testSplittingLargeRangesBy2() {
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(0, Long.MAX_VALUE), 2);
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-1000000000L, Long.MAX_VALUE), 2);
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(Long.MIN_VALUE, Long.MAX_VALUE), 2);
	}

	@Test
	public void testSplittingTooSmallRanges() {
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(0, 0), 2);
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-5, -5), 2);
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(-5, -4), 3);
		testSplitting(new org.apache.flink.util.LongValueSequenceIterator(10, 15), 10);
	}

	private static void testSplitting(org.apache.flink.util.LongValueSequenceIterator iter, int numSplits) {
		org.apache.flink.util.LongValueSequenceIterator[] splits = iter.split(numSplits);

		assertEquals(numSplits, splits.length);

		// test start and end of range
		assertEquals(iter.getCurrent(), splits[0].getCurrent());
		assertEquals(iter.getTo(), splits[numSplits - 1].getTo());

		// test continuous range
		for (int i = 1; i < splits.length; i++) {
			assertEquals(splits[i - 1].getTo() + 1, splits[i].getCurrent());
		}

		testMaxSplitDiff(splits);
	}

	private static void testMaxSplitDiff(org.apache.flink.util.LongValueSequenceIterator[] iters) {
		long minSplitSize = Long.MAX_VALUE;
		long maxSplitSize = Long.MIN_VALUE;

		for (LongValueSequenceIterator iter : iters) {
			long diff;
			if (iter.getTo() < iter.getCurrent()) {
				diff = 0;
			} else {
				diff = iter.getTo() - iter.getCurrent();
			}
			if (diff < 0) {
				diff = Long.MAX_VALUE;
			}

			minSplitSize = Math.min(minSplitSize, diff);
			maxSplitSize = Math.max(maxSplitSize, diff);
		}

		assertTrue(maxSplitSize == minSplitSize || maxSplitSize - 1 == minSplitSize);
	}

}
