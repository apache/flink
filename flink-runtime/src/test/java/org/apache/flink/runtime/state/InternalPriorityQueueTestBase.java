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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Testbase for implementations of {@link InternalPriorityQueue}.
 */
public abstract class InternalPriorityQueueTestBase extends TestLogger {

	protected static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, 2);
	protected static final KeyExtractorFunction<TestElement> KEY_EXTRACTOR_FUNCTION = TestElement::getKey;
	protected static final Comparator<TestElement> TEST_ELEMENT_COMPARATOR =
		Comparator.comparingLong(TestElement::getPriority).thenComparingLong(TestElement::getKey);

	protected static void insertRandomTimers(
		@Nonnull InternalPriorityQueue<TestElement> priorityQueue,
		@Nonnull Set<TestElement> checkSet,
		int count) {

		ThreadLocalRandom localRandom = ThreadLocalRandom.current();

		final int numUniqueKeys = Math.max(count / 4, 64);

		long duplicatePriority = Long.MIN_VALUE;

		for (int i = 0; i < count; ++i) {
			TestElement element;
			do {
				long elementPriority;
				if (duplicatePriority == Long.MIN_VALUE) {
					elementPriority = localRandom.nextLong();
				} else {
					elementPriority = duplicatePriority;
					duplicatePriority = Long.MIN_VALUE;
				}
				element = new TestElement(localRandom.nextInt(numUniqueKeys), elementPriority);
			} while (!checkSet.add(element));

			if (localRandom.nextInt(10) == 0) {
				duplicatePriority = element.getPriority();
			}

			final boolean headChangedIndicated = priorityQueue.add(element);
			if (element.equals(priorityQueue.peek())) {
				Assert.assertTrue(headChangedIndicated);
			}
		}
		Assert.assertEquals(count, priorityQueue.size());
	}

	@Test
	public void testPeekPollOrder() {
		final int initialCapacity = 4;
		final int testSize = 1000;
		InternalPriorityQueue<TestElement> priorityQueue =
			newPriorityQueue(initialCapacity);
		HashSet<TestElement> checkSet = new HashSet<>(testSize);

		insertRandomTimers(priorityQueue, checkSet, testSize);

		long lastPriorityValue = Long.MIN_VALUE;
		int lastSize = priorityQueue.size();
		Assert.assertEquals(testSize, lastSize);
		TestElement testElement;
		while ((testElement = priorityQueue.peek()) != null) {
			Assert.assertFalse(priorityQueue.isEmpty());
			Assert.assertEquals(lastSize, priorityQueue.size());
			Assert.assertEquals(testElement, priorityQueue.poll());
			Assert.assertTrue(checkSet.remove(testElement));
			Assert.assertTrue(testElement.getPriority() >= lastPriorityValue);
			lastPriorityValue = testElement.getPriority();
			--lastSize;
		}

		Assert.assertTrue(priorityQueue.isEmpty());
		Assert.assertEquals(0, priorityQueue.size());
		Assert.assertEquals(0, checkSet.size());
	}

	@Test
	public void testStopInsertMixKeepsOrder() {

		InternalPriorityQueue<TestElement> priorityQueue = newPriorityQueue(3);

		final int testSize = 128;
		HashSet<TestElement> checkSet = new HashSet<>(testSize);

		insertRandomTimers(priorityQueue, checkSet, testSize);

		// check that the whole set is still in order
		while (!checkSet.isEmpty()) {

			Iterator<TestElement> iterator = checkSet.iterator();
			TestElement element = iterator.next();
			iterator.remove();

			boolean removesHead = element.equals(priorityQueue.peek());
			if (removesHead) {
				Assert.assertTrue(priorityQueue.remove(element));
			} else {
				priorityQueue.remove(element);
			}
			Assert.assertEquals(checkSet.size(), priorityQueue.size());

			long lastPriorityValue = Long.MIN_VALUE;

			while ((element = priorityQueue.poll()) != null) {
				Assert.assertTrue(element.getPriority() >= lastPriorityValue);
				lastPriorityValue = element.getPriority();
			}

			Assert.assertTrue(priorityQueue.isEmpty());

			priorityQueue.addAll(checkSet);
		}
	}

	@Test
	public void testPoll() {
		InternalPriorityQueue<TestElement> priorityQueue = newPriorityQueue(3);

		Assert.assertNull(priorityQueue.poll());

		final int testSize = 345;
		HashSet<TestElement> checkSet = new HashSet<>(testSize);
		insertRandomTimers(priorityQueue, checkSet, testSize);

		long lastPriorityValue = Long.MIN_VALUE;
		while (!priorityQueue.isEmpty()) {
			TestElement removed = priorityQueue.poll();
			Assert.assertNotNull(removed);
			Assert.assertTrue(checkSet.remove(removed));
			Assert.assertTrue(removed.getPriority() >= lastPriorityValue);
			lastPriorityValue = removed.getPriority();
		}
		Assert.assertTrue(checkSet.isEmpty());

		Assert.assertNull(priorityQueue.poll());
	}

	@Test
	public void testIsEmpty() {
		InternalPriorityQueue<TestElement> priorityQueue =
			newPriorityQueue(1);

		Assert.assertTrue(priorityQueue.isEmpty());

		Assert.assertTrue(priorityQueue.add(new TestElement(4711L, 42L)));
		Assert.assertFalse(priorityQueue.isEmpty());

		priorityQueue.poll();
		Assert.assertTrue(priorityQueue.isEmpty());
	}

	@Test
	public void testBulkAddRestoredTimers() throws Exception {
		final int testSize = 10;
		HashSet<TestElement> elementSet = new HashSet<>(testSize);
		for (int i = 0; i < testSize; ++i) {
			elementSet.add(new TestElement(i, i));
		}

		List<TestElement> twoTimesElementSet = new ArrayList<>(elementSet.size() * 2);

		for (TestElement testElement : elementSet) {
			twoTimesElementSet.add(testElement.deepCopy());
			twoTimesElementSet.add(testElement.deepCopy());
		}

		InternalPriorityQueue<TestElement> priorityQueue =
			newPriorityQueue(1);

		priorityQueue.addAll(twoTimesElementSet);
		priorityQueue.addAll(elementSet);

		final int expectedSize = testSetSemantics() ? elementSet.size() : 3 * elementSet.size();

		Assert.assertEquals(expectedSize, priorityQueue.size());
		try (final CloseableIterator<TestElement> iterator = priorityQueue.iterator()) {
			while (iterator.hasNext()) {
				if (testSetSemantics()) {
					Assert.assertTrue(elementSet.remove(iterator.next()));
				} else {
					Assert.assertTrue(elementSet.contains(iterator.next()));
				}
			}
		}
		if (testSetSemantics()) {
			Assert.assertTrue(elementSet.isEmpty());
		}
	}

	@Test
	public void testIterator() throws Exception {
		InternalPriorityQueue<TestElement> priorityQueue =
			newPriorityQueue(1);

		// test empty iterator
		try (CloseableIterator<TestElement> iterator = priorityQueue.iterator()) {
			Assert.assertFalse(iterator.hasNext());
			try {
				iterator.next();
				Assert.fail();
			} catch (NoSuchElementException ignore) {
			}
		}

		// iterate some data
		final int testSize = 10;
		HashSet<TestElement> checkSet = new HashSet<>(testSize);
		insertRandomTimers(priorityQueue, checkSet, testSize);
		try (CloseableIterator<TestElement> iterator = priorityQueue.iterator()) {
			while (iterator.hasNext()) {
				Assert.assertTrue(checkSet.remove(iterator.next()));
			}
			Assert.assertTrue(checkSet.isEmpty());
		}
	}

	@Test
	public void testAdd() {
		InternalPriorityQueue<TestElement> priorityQueue =
			newPriorityQueue(1);

		TestElement lowPrioElement = new TestElement(4711L, 42L);
		TestElement highPrioElement = new TestElement(815L, 23L);
		Assert.assertTrue(priorityQueue.add(lowPrioElement));
		if (testSetSemantics()) {
			priorityQueue.add(lowPrioElement.deepCopy());
		}
		Assert.assertEquals(1, priorityQueue.size());
		Assert.assertTrue(priorityQueue.add(highPrioElement));
		Assert.assertEquals(2, priorityQueue.size());
		Assert.assertEquals(highPrioElement, priorityQueue.poll());
		Assert.assertEquals(1, priorityQueue.size());
		Assert.assertEquals(lowPrioElement, priorityQueue.poll());
		Assert.assertEquals(0, priorityQueue.size());
	}

	@Test
	public void testRemove() {
		InternalPriorityQueue<TestElement> priorityQueue =
			newPriorityQueue(1);

		final long key = 4711L;
		final long priorityValue = 42L;
		final TestElement testElement = new TestElement(key, priorityValue);
		if (testSetSemantics()) {
			Assert.assertFalse(priorityQueue.remove(testElement));
		}
		Assert.assertTrue(priorityQueue.add(testElement));
		Assert.assertTrue(priorityQueue.remove(testElement));
		if (testSetSemantics()) {
			Assert.assertFalse(priorityQueue.remove(testElement));
		}
		Assert.assertTrue(priorityQueue.isEmpty());
	}

	protected abstract InternalPriorityQueue<TestElement> newPriorityQueue(int initialCapacity);

	protected abstract boolean testSetSemantics();

	/**
	 * Payload for usage in the test.
	 */
	protected static class TestElement implements HeapPriorityQueueElement {

		private final long key;
		private final long priority;
		private int internalIndex;

		public TestElement(long key, long priority) {
			this.key = key;
			this.priority = priority;
			this.internalIndex = NOT_CONTAINED;
		}

		public long getKey() {
			return key;
		}

		public long getPriority() {
			return priority;
		}

		@Override
		public int getInternalIndex() {
			return internalIndex;
		}

		@Override
		public void setInternalIndex(int newIndex) {
			internalIndex = newIndex;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TestElement that = (TestElement) o;
			return getKey() == that.getKey() &&
				getPriority() == that.getPriority();
		}

		@Override
		public int hashCode() {
			return Objects.hash(getKey(), getPriority());
		}

		public TestElement deepCopy() {
			return new TestElement(key, priority);
		}
	}

	/**
	 * Serializer for {@link TestElement}. The serialization format produced by this serializer allows lexicographic
	 * ordering by {@link TestElement#getPriority}.
	 */
	protected static class TestElementSerializer extends TypeSerializer<TestElement> {

		public static final TestElementSerializer INSTANCE = new TestElementSerializer();

		private TestElementSerializer() {
		}

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public TypeSerializer<TestElement> duplicate() {
			return this;
		}

		@Override
		public TestElement createInstance() {
			throw new UnsupportedOperationException();
		}

		@Override
		public TestElement copy(TestElement from) {
			return new TestElement(from.key, from.priority);
		}

		@Override
		public TestElement copy(TestElement from, TestElement reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return 2 * Long.BYTES;
		}

		@Override
		public void serialize(TestElement record, DataOutputView target) throws IOException {
			// serialize priority first, so that we have correct order in RocksDB. We flip the sign bit for correct
			// lexicographic order.
			target.writeLong(MathUtils.flipSignBit(record.getPriority()));
			target.writeLong(record.getKey());
		}

		@Override
		public TestElement deserialize(DataInputView source) throws IOException {
			long prio = MathUtils.flipSignBit(source.readLong());
			long key = source.readLong();
			return new TestElement(key, prio);
		}

		@Override
		public TestElement deserialize(TestElement reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			serialize(deserialize(source), target);
		}

		@Override
		public boolean equals(Object obj) {
			return false;
		}

		@Override
		public boolean canEqual(Object obj) {
			return false;
		}

		@Override
		public int hashCode() {
			return 4711;
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CompatibilityResult<TestElement> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			throw new UnsupportedOperationException();
		}
	}
}
