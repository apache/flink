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

package org.apache.flink.runtime.io.network.partition;

import org.junit.Test;

import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests PrioritizedDeque.
 *
 * <p>Note that some tests make use of the {@link java.lang.Integer.IntegerCache} for improved readability.
 */
public class PrioritizedDequeTest {

	@Test
	public void testPrioritizeOnAdd() {
		final PrioritizedDeque<Integer> deque = new PrioritizedDeque<>();

		deque.add(0);
		deque.add(1);
		deque.add(2);
		deque.add(3);
		deque.add(3, true, true);

		assertArrayEquals(new Integer[] { 3, 0, 1, 2 }, deque.asUnmodifiableCollection().toArray(new Integer[0]));
	}

	@Test
	public void testPrioritize() {
		final PrioritizedDeque<Integer> deque = new PrioritizedDeque<>();

		deque.add(0);
		deque.add(1);
		deque.add(2);
		deque.add(3);
		deque.prioritize(3);

		assertArrayEquals(new Integer[] { 3, 0, 1, 2 }, deque.asUnmodifiableCollection().toArray(new Integer[0]));
	}

	@Test
	public void testGetAndRemove() {
		final PrioritizedDeque<Integer> deque = new PrioritizedDeque<>();

		deque.add(0);
		deque.add(1);
		deque.add(2);
		deque.add(1);
		deque.add(3);

		assertEquals(1, deque.getAndRemove(v -> v == 1).intValue());
		assertArrayEquals(new Integer[] { 0, 2, 1, 3 }, deque.asUnmodifiableCollection().toArray(new Integer[0]));
		assertEquals(1, deque.getAndRemove(v -> v == 1).intValue());
		assertArrayEquals(new Integer[] { 0, 2, 3 }, deque.asUnmodifiableCollection().toArray(new Integer[0]));
		try {
			int removed = deque.getAndRemove(v -> v == 1);
			fail(String.format("This should not happen. Item [%s] was removed, but it shouldn't be found", removed));
		}
		catch (NoSuchElementException ex) {
			// expected
		}
	}
}
