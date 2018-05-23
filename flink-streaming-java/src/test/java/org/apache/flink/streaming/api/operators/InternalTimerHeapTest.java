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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Tests for {@link InternalTimerHeap}.
 */
public class InternalTimerHeapTest {

	private static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, 1);

	private static void insertRandomTimers(
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue,
		int count) {
		insertRandomTimers(timerPriorityQueue, null, count);
	}

	private static void insertRandomTimers(
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue,
		Set<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet,
		int count) {

		ThreadLocalRandom localRandom = ThreadLocalRandom.current();

		for (int i = 0; i < count; ++i) {
			TimerHeapInternalTimer<Integer, VoidNamespace> timer =
				new TimerHeapInternalTimer<>(localRandom.nextLong(), i, VoidNamespace.INSTANCE);
			if (checkSet != null) {
				Preconditions.checkState(checkSet.add(timer));
			}
			Assert.assertTrue(timerPriorityQueue.add(timer));
		}
	}

	private static InternalTimerHeap<Integer, VoidNamespace> newPriorityQueue(int initialCapacity) {
		return new InternalTimerHeap<>(
			initialCapacity,
			KEY_GROUP_RANGE,
			KEY_GROUP_RANGE.getNumberOfKeyGroups());
	}

	@Test
	public void testCombined() {
		final int initialCapacity = 4;
		final int testSize = 1000;
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue = newPriorityQueue(initialCapacity);
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);

		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

		long lastTimestamp = Long.MIN_VALUE;
		int lastSize = timerPriorityQueue.size();
		Assert.assertEquals(testSize, lastSize);
		TimerHeapInternalTimer<Integer, VoidNamespace> timer;
		while ((timer = timerPriorityQueue.peek()) != null) {
			Assert.assertFalse(timerPriorityQueue.isEmpty());
			Assert.assertEquals(lastSize, timerPriorityQueue.size());
			Assert.assertEquals(timer, timerPriorityQueue.poll());
			Assert.assertTrue(checkSet.remove(timer));
			Assert.assertTrue(timer.getTimestamp() >= lastTimestamp);
			lastTimestamp = timer.getTimestamp();
			--lastSize;
		}

		Assert.assertTrue(timerPriorityQueue.isEmpty());
		Assert.assertEquals(0, timerPriorityQueue.size());
		Assert.assertEquals(0, checkSet.size());
	}

	@Test
	public void testAdd() {
		testAddOfferCommon(InternalTimerHeap<Integer, VoidNamespace>::add);
	}

	@Test
	public void testOffer() {
		testAddOfferCommon(InternalTimerHeap<Integer, VoidNamespace>::offer);
	}

	@Test
	public void testRemove() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue = newPriorityQueue(3);

		try {
			timerPriorityQueue.remove();
			Assert.fail();
		} catch (NoSuchElementException ignore) {
		}

		testRemovePollCommon(timerPriorityQueue, InternalTimerHeap::remove);

		try {
			timerPriorityQueue.remove();
			Assert.fail();
		} catch (NoSuchElementException ignore) {
		}
	}

	@Test
	public void testPoll() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue = newPriorityQueue(3);

		Assert.assertNull(timerPriorityQueue.poll());

		testRemovePollCommon(timerPriorityQueue, InternalTimerHeap::poll);

		Assert.assertNull(timerPriorityQueue.poll());
	}

	@Test
	public void testRemoveTimer() {

		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue = newPriorityQueue(3);

		final int testSize = 345;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);

		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

		// check that the whole set is still in order
		while (!checkSet.isEmpty()) {

			Iterator<TimerHeapInternalTimer<Integer, VoidNamespace>> iterator = checkSet.iterator();
			InternalTimer<Integer, VoidNamespace> timer = iterator.next();
			iterator.remove();
			Assert.assertTrue(timerPriorityQueue.remove(timer));
			Assert.assertEquals(checkSet.size(), timerPriorityQueue.size());

			long lastTimestamp = Long.MIN_VALUE;

			while ((timer = timerPriorityQueue.poll()) != null) {
				Assert.assertTrue(timer.getTimestamp() >= lastTimestamp);
				lastTimestamp = timer.getTimestamp();
			}

			Assert.assertTrue(timerPriorityQueue.isEmpty());

			timerPriorityQueue.addAll(checkSet);
		}
	}

	@Test
	public void testPeek() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		Assert.assertNull(timerPriorityQueue.peek());

		testPeekElementCommon(timerPriorityQueue, InternalTimerHeap::peek);
	}

	@Test
	public void testElement() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);
		try {
			timerPriorityQueue.element();
			Assert.fail();
		} catch (NoSuchElementException ignore) {
		}
		testPeekElementCommon(timerPriorityQueue, InternalTimerHeap::element);
	}

	@Test
	public void testIsEmpty() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		Assert.assertTrue(timerPriorityQueue.isEmpty());

		TimerHeapInternalTimer<Integer, VoidNamespace> timer =
			new TimerHeapInternalTimer<>(42L, 4711, VoidNamespace.INSTANCE);

		timerPriorityQueue.add(timer);
		Assert.assertFalse(timerPriorityQueue.isEmpty());

		timerPriorityQueue.poll();
		Assert.assertTrue(timerPriorityQueue.isEmpty());
	}

	@Test
	public void testContains() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);
		TimerHeapInternalTimer<Integer, VoidNamespace> timer =
			new TimerHeapInternalTimer<>(42L, 4711, VoidNamespace.INSTANCE);

		Assert.assertFalse(timerPriorityQueue.contains(timer));

		timerPriorityQueue.add(timer);
		Assert.assertTrue(timerPriorityQueue.contains(timer));

		timerPriorityQueue.remove(timer);
		Assert.assertFalse(timerPriorityQueue.contains(timer));
	}

	@Test
	public void testAddAll() {
		final int testSize = 10;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> timerSet = new HashSet<>(testSize);
		for (int i = 0; i < testSize; ++i) {
			timerSet.add(new TimerHeapInternalTimer<>(i, i, VoidNamespace.INSTANCE));
		}

		List<TimerHeapInternalTimer<Integer, VoidNamespace>> twoTimesTimerSet = new ArrayList<>(timerSet.size() * 2);
		twoTimesTimerSet.addAll(timerSet);
		twoTimesTimerSet.addAll(timerSet);

		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		Assert.assertTrue(timerPriorityQueue.addAll(twoTimesTimerSet));
		Assert.assertFalse(timerPriorityQueue.addAll(twoTimesTimerSet));

		Assert.assertEquals(timerSet.size(), timerPriorityQueue.size());

		for (TimerHeapInternalTimer<Integer, VoidNamespace> timer : timerPriorityQueue) {
			Assert.assertTrue(timerSet.remove(timer));
		}

		Assert.assertTrue(timerSet.isEmpty());
	}

	@Test
	public void testToArray() {
		final int testSize = 10;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		Assert.assertEquals(0, timerPriorityQueue.toArray().length);

		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

		Object[] toArray = timerPriorityQueue.toArray();
		Assert.assertEquals(timerPriorityQueue.size(), toArray.length);

		for (Object o : toArray) {
			if (o instanceof TimerHeapInternalTimer) {
				Assert.assertTrue(checkSet.remove(o));
			}
		}

		Assert.assertTrue(checkSet.isEmpty());
	}

	@Test
	public void testToArrayGeneric() {
		final int testSize = 10;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		Assert.assertEquals(0, timerPriorityQueue.toArray().length);

		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

		int[] arraySizes = new int[]{0, timerPriorityQueue.size(), timerPriorityQueue.size() + 2};

		for (int arraySize : arraySizes) {
			HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSetCopy = new HashSet<>(checkSet);
			TimerHeapInternalTimer[] toArray = timerPriorityQueue.toArray(new TimerHeapInternalTimer[arraySize]);
			Assert.assertEquals(Math.max(timerPriorityQueue.size(), arraySize), toArray.length);

			for (TimerHeapInternalTimer o : toArray) {
				if (o != null) {
					Assert.assertTrue(checkSetCopy.remove(o));
				}
			}
			Assert.assertTrue(checkSetCopy.isEmpty());
		}
	}

	@Test
	public void testRemoveAll() {
		final int testSize = 10;
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);
		timerPriorityQueue.removeAll(Collections.<TimerHeapInternalTimer<Integer, VoidNamespace>>emptyList());
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);
		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

	}

	@Test
	public void testRetainAll() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);
		try {
			timerPriorityQueue.retainAll(Collections.emptyList());
			Assert.fail();
		} catch (UnsupportedOperationException ignore) {
		}
	}

	@Test
	public void testIterator() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		// test empty iterator
		Iterator<TimerHeapInternalTimer<Integer, VoidNamespace>> iterator = timerPriorityQueue.iterator();
		Assert.assertFalse(iterator.hasNext());
		try {
			iterator.next();
			Assert.fail();
		} catch (NoSuchElementException ignore) {
		}

		// iterate some data
		final int testSize = 10;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);
		insertRandomTimers(timerPriorityQueue, checkSet, testSize);
		iterator = timerPriorityQueue.iterator();
		while (iterator.hasNext()) {
			Assert.assertTrue(checkSet.remove(iterator.next()));
		}
		Assert.assertTrue(checkSet.isEmpty());

		// test remove is not supported
		try {
			iterator.remove();
			Assert.fail();
		} catch (UnsupportedOperationException ignore) {
		}
	}

	@Test
	public void testClear() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		int count = 10;
		insertRandomTimers(timerPriorityQueue, count);
		Assert.assertEquals(count, timerPriorityQueue.size());
		timerPriorityQueue.clear();
		Assert.assertEquals(0, timerPriorityQueue.size());
	}

	@Test
	public void testScheduleTimer() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		final long timestamp = 42L;
		final Integer key = 4711;
		Assert.assertTrue(timerPriorityQueue.scheduleTimer(timestamp, key, VoidNamespace.INSTANCE));
		Assert.assertFalse(timerPriorityQueue.scheduleTimer(timestamp, key, VoidNamespace.INSTANCE));
		Assert.assertEquals(1, timerPriorityQueue.size());
		final InternalTimer<Integer, VoidNamespace> timer = timerPriorityQueue.remove();
		Assert.assertEquals(timestamp, timer.getTimestamp());
		Assert.assertEquals(key, timer.getKey());
		Assert.assertEquals(VoidNamespace.INSTANCE, timer.getNamespace());
	}

	@Test
	public void testStopTimer() {
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue =
			newPriorityQueue(1);

		final long timestamp = 42L;
		final Integer key = 4711;
		Assert.assertFalse(timerPriorityQueue.stopTimer(timestamp, key, VoidNamespace.INSTANCE));
		Assert.assertTrue(timerPriorityQueue.scheduleTimer(timestamp, key, VoidNamespace.INSTANCE));
		Assert.assertTrue(timerPriorityQueue.stopTimer(timestamp, key, VoidNamespace.INSTANCE));
		Assert.assertFalse(timerPriorityQueue.stopTimer(timestamp, key, VoidNamespace.INSTANCE));
		Assert.assertTrue(timerPriorityQueue.isEmpty());
	}

	private void testAddOfferCommon(
		BiFunction<InternalTimerHeap, TimerHeapInternalTimer<Integer, VoidNamespace>, Boolean> testMethod) {

		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue = newPriorityQueue(1);

		Assert.assertTrue(timerPriorityQueue.isEmpty());

		TimerHeapInternalTimer<Integer, VoidNamespace> timer1 =
			new TimerHeapInternalTimer<>(42L, 4711, VoidNamespace.INSTANCE);
		TimerHeapInternalTimer<Integer, VoidNamespace> timer2 =
			new TimerHeapInternalTimer<>(43L, 4712, VoidNamespace.INSTANCE);

		// add first timer
		Assert.assertTrue(testMethod.apply(timerPriorityQueue, timer1));
		Assert.assertEquals(1, timerPriorityQueue.size());

		// add second timer
		Assert.assertTrue(testMethod.apply(timerPriorityQueue, timer2));
		Assert.assertEquals(2, timerPriorityQueue.size());

		// check adding first timer again, duplicate should not be added
		Assert.assertFalse(testMethod.apply(timerPriorityQueue, timer1));
		Assert.assertEquals(2, timerPriorityQueue.size());
	}

	private void testPeekElementCommon(
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue,
		Function<InternalTimerHeap, TimerHeapInternalTimer<Integer, VoidNamespace>> testFun) {

		TimerHeapInternalTimer<Integer, VoidNamespace> timer1 =
			new TimerHeapInternalTimer<>(42L, 4711, VoidNamespace.INSTANCE);
		timerPriorityQueue.add(timer1);

		Assert.assertEquals(timer1, testFun.apply(timerPriorityQueue));

		TimerHeapInternalTimer<Integer, VoidNamespace> timer2 =
			new TimerHeapInternalTimer<>(43L, 4712, VoidNamespace.INSTANCE);
		timerPriorityQueue.add(timer2);

		Assert.assertEquals(timer1, testFun.apply(timerPriorityQueue));

		TimerHeapInternalTimer<Integer, VoidNamespace> timer3 =
			new TimerHeapInternalTimer<>(41L, 4712, VoidNamespace.INSTANCE);
		timerPriorityQueue.add(timer3);

		Assert.assertEquals(timer3, testFun.apply(timerPriorityQueue));
		Assert.assertEquals(3, timerPriorityQueue.size());
	}

	private void testRemovePollCommon(
		InternalTimerHeap<Integer, VoidNamespace> timerPriorityQueue,
		Function<InternalTimerHeap<Integer, VoidNamespace>, TimerHeapInternalTimer<Integer, VoidNamespace>> fun) {

		final int testSize = 345;
		HashSet<TimerHeapInternalTimer<Integer, VoidNamespace>> checkSet = new HashSet<>(testSize);
		insertRandomTimers(timerPriorityQueue, checkSet, testSize);

		long lastTimestamp = Long.MIN_VALUE;
		while (!timerPriorityQueue.isEmpty()) {
			TimerHeapInternalTimer<Integer, VoidNamespace> removed = fun.apply(timerPriorityQueue);
			Assert.assertTrue(checkSet.remove(removed));
			Assert.assertTrue(removed.getTimestamp() >= lastTimestamp);
			lastTimestamp = removed.getTimestamp();
		}
		Assert.assertTrue(checkSet.isEmpty());
	}
}
