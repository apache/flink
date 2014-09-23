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

package org.apache.flink.runtime.jobmanager.scheduler;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test for the basic functionality of the {@link LifoSetQueue}.
 */
public class LifoSetQueueTest {

	@Test
	public void testSizeAddPollAndPeek() {
		try {
			LifoSetQueue<Integer> queue = new LifoSetQueue<Integer>();
			
			// empty queue
			assertEquals(0, queue.size());
			assertNull(queue.poll());
			assertNull(queue.peek());
			
			// add some elements
			assertTrue(queue.add(1));
			assertTrue(queue.offer(2));
			assertTrue(queue.offer(3));
			assertEquals(3, queue.size());
			
			assertEquals(3, queue.peek().intValue());
			
			// prevent duplicates. note that the methods return true, because no capacity constraint is violated
			assertTrue(queue.add(1));
			assertTrue(queue.offer(1));
			assertTrue(queue.add(3));
			assertTrue(queue.offer(3));
			assertTrue(queue.add(2));
			assertTrue(queue.offer(2));
			assertEquals(3, queue.size());
			
			// peek and poll some elements
			assertEquals(3, queue.peek().intValue());
			assertEquals(3, queue.size());
			assertEquals(3, queue.poll().intValue());
			assertEquals(2, queue.size());
			assertEquals(2, queue.peek().intValue());
			assertEquals(2, queue.size());
			assertEquals(2, queue.poll().intValue());
			assertEquals(1, queue.size());
			assertEquals(1, queue.peek().intValue());
			assertEquals(1, queue.size());
			assertEquals(1, queue.poll().intValue());
			assertEquals(0, queue.size());
			assertTrue(queue.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " : " + e.getMessage());
		}
	}
	
	/**
	 * Remove is tricky, because it goes through the iterator and calls remove() on the iterator.
	 */
	@Test
	public void testRemove() {
		try {
			LifoSetQueue<String> queue = new LifoSetQueue<String>();
			queue.add("1");
			queue.add("2");
			queue.add("3");
			queue.add("4");
			queue.add("5");
			queue.add("6");
			queue.add("7");
			
			assertEquals(7, queue.size());
			assertEquals("7", queue.peek());
			
			// remove non-existing
			assertFalse(queue.remove("8"));
			
			// remove the last
			assertTrue(queue.remove("7"));
			// remove the first
			assertTrue(queue.remove("1"));
			// remove in the middle
			assertTrue(queue.remove("3"));
			
			assertEquals(4, queue.size());
			
			// check that we can re-add the removed elements
			assertTrue(queue.add("1"));
			assertTrue(queue.add("7"));
			assertTrue(queue.add("3"));
			assertEquals(7, queue.size());
			
			// check the order
			assertEquals("3", queue.poll());
			assertEquals("7", queue.poll());
			assertEquals("1", queue.poll());
			assertEquals("6", queue.poll());
			assertEquals("5", queue.poll());
			assertEquals("4", queue.poll());
			assertEquals("2", queue.poll());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " : " + e.getMessage());
		}
	}
}
