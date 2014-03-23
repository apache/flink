/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class NumberSequenceIteratorTest {

	@Test
	public void testSplitRegular() {
		testSplitting(new NumberSequenceIterator(0, 10), 2);
		testSplitting(new NumberSequenceIterator(100, 100000), 7);
		testSplitting(new NumberSequenceIterator(-100, 0), 5);
		testSplitting(new NumberSequenceIterator(-100, 100), 3);
	}
	
	@Test
	public void testSplittingLargeRangesBy2() {
		testSplitting(new NumberSequenceIterator(0, Long.MAX_VALUE), 2);
		testSplitting(new NumberSequenceIterator(-1000000000L, Long.MAX_VALUE), 2);
		testSplitting(new NumberSequenceIterator(Long.MIN_VALUE, Long.MAX_VALUE), 2);
	}
	
	@Test
	public void testSplittingTooSmallRanges() {
		testSplitting(new NumberSequenceIterator(0, 0), 2);
		testSplitting(new NumberSequenceIterator(-5, -5), 2);
		testSplitting(new NumberSequenceIterator(-5, -4), 3);
		testSplitting(new NumberSequenceIterator(10, 15), 10);
	}
	
	private static final void testSplitting(NumberSequenceIterator iter, int numSplits) {
		NumberSequenceIterator[] splits = iter.split(numSplits);
		
		assertEquals(numSplits, splits.length);
		
		assertEquals(iter.getCurrent(), splits[0].getCurrent());
		assertEquals(iter.getTo(), splits[numSplits-1].getTo());
		
		testMaxSplitDiff(splits);
	}
	
	
	private static final void testMaxSplitDiff(NumberSequenceIterator[] iters) {
		long minSplitSize = Long.MAX_VALUE;
		long maxSplitSize = Long.MIN_VALUE;
		
		for (NumberSequenceIterator iter : iters) {
			long diff = iter.getTo() - iter.getCurrent();
			if (diff < 0)
				diff = Long.MAX_VALUE;
			
			minSplitSize = Math.min(minSplitSize, diff);
			maxSplitSize = Math.max(maxSplitSize, diff);
		}
		
		assertTrue(maxSplitSize == minSplitSize || maxSplitSize-1 == minSplitSize);
	}

}
