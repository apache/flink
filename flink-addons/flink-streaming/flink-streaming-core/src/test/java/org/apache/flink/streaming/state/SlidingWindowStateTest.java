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

package org.apache.flink.streaming.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.junit.Test;

public class SlidingWindowStateTest {

	private final static int SLIDING_BATCH_SIZE = 3;
	private final static int SLIDE_SIZE = 2;
	private static final int UNIT = 1;

	@SuppressWarnings("unchecked")
	@Test
	public void basicSlidingWindowStateTest() {
		SlidingWindowState<Integer> state = new SlidingWindowState<Integer>(SLIDING_BATCH_SIZE,
				SLIDE_SIZE, UNIT);
		state.pushBack(Arrays.asList(new StreamRecord<Integer>().setObject(0)));
		state.pushBack(Arrays.asList(new StreamRecord<Integer>().setObject(1)));
		assertEquals(false, state.isFull());
		state.pushBack(Arrays.asList(new StreamRecord<Integer>().setObject(2)));
		assertTrue(state.isFull());

		SlidingWindowStateIterator<Integer> iterator = state.getIterator();

		SortedSet<Integer> actualSet = new TreeSet<Integer>();
		while (iterator.hasNext()) {
			actualSet.add(iterator.next());
		}
		assertEquals(getExpectedSet(0, 2), actualSet);
		actualSet.clear();

		state.pushBack(Arrays.asList(new StreamRecord<Integer>().setObject(3)));
		assertEquals(false, state.isEmittable());
		state.pushBack(Arrays.asList(new StreamRecord<Integer>().setObject(4)));
		assertTrue(state.isEmittable());

		iterator = state.getIterator();

		while (iterator.hasNext()) {
			actualSet.add(iterator.next());
		}
		assertEquals(getExpectedSet(2, 4), actualSet);
	}
	
	private final static int WINDOW_SIZE = 10;
	private final static int WINDOW_SLIDE_SIZE = 4;
	private static final int WINDOW_UNIT = 2;
	
	@SuppressWarnings("unchecked")
	@Test
	public void slidingWithGreaterUnit() {
		SlidingWindowState<Integer> state = new SlidingWindowState<Integer>(WINDOW_SIZE,
				WINDOW_SLIDE_SIZE, WINDOW_UNIT);
		state.pushBack(new ArrayList<StreamRecord<Integer>>());
		state.pushBack(Arrays.asList(new StreamRecord<Integer>().setObject(1)));
		state.pushBack(new ArrayList<StreamRecord<Integer>>());
		state.pushBack(Arrays.asList(new StreamRecord<Integer>().setObject(2), new StreamRecord<Integer>().setObject(3)));
		state.pushBack(new ArrayList<StreamRecord<Integer>>());

		SortedSet<Integer> actualSet = new TreeSet<Integer>();
		SlidingWindowStateIterator<Integer> iterator = state.getIterator();
		
		iterator.hasNext();
		iterator.hasNext();
		while (iterator.hasNext()) {
			iterator.hasNext();
			iterator.hasNext();
			actualSet.add(iterator.next());
			iterator.hasNext();
			iterator.hasNext();
		}
		
		assertEquals(getExpectedSet(1, 3), actualSet);
		actualSet.clear();
		
		Iterator<StreamRecord<Integer>> streamRecordIterator = state.getStreamRecordIterator();
		
		streamRecordIterator.hasNext();
		streamRecordIterator.hasNext();
		while (streamRecordIterator.hasNext()) {
			streamRecordIterator.hasNext();
			streamRecordIterator.hasNext();
			actualSet.add(streamRecordIterator.next().getObject());
			streamRecordIterator.hasNext();
			streamRecordIterator.hasNext();
		}
		
		assertEquals(getExpectedSet(1, 3), actualSet);
	}

	private SortedSet<Integer> getExpectedSet(int from, int to) {
		SortedSet<Integer> expectedSet = new TreeSet<Integer>();
		for (int i = from; i <= to; i++) {
			expectedSet.add(i);
		}
		return expectedSet;
	}

}
