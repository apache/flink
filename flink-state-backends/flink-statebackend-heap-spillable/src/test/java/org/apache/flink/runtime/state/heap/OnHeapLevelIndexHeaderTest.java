/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.state.heap.SkipListUtils.DEFAULT_LEVEL;
import static org.apache.flink.runtime.state.heap.SkipListUtils.MAX_LEVEL;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;

/**
 * Tests for {@link OnHeapLevelIndexHeader}.
 */
public class OnHeapLevelIndexHeaderTest extends TestLogger {
	private static final ThreadLocalRandom random = ThreadLocalRandom.current();

	private OnHeapLevelIndexHeader heapHeadIndex;

	@Before
	public void setUp() {
		heapHeadIndex = new OnHeapLevelIndexHeader();
	}

	@Test
	public void testInitStatus() {
		Assert.assertEquals(1, heapHeadIndex.getLevel());
		Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelIndex().length);
		for (long node : heapHeadIndex.getLevelIndex()) {
			Assert.assertEquals(NIL_NODE, node);
		}
		for (int level = 0; level <= heapHeadIndex.getLevel(); level++) {
			Assert.assertEquals(NIL_NODE, heapHeadIndex.getNextNode(level));
		}
	}

	@Test
	public void testNormallyUpdateLevel() {
		int level = heapHeadIndex.getLevel();
		// update level to no more than init max level
		for (; level <= DEFAULT_LEVEL; level++) {
			heapHeadIndex.updateLevel(level);
			Assert.assertEquals(level, heapHeadIndex.getLevel());
			Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelIndex().length);
		}
		// update level to trigger scale up
		heapHeadIndex.updateLevel(level);
		Assert.assertEquals(level, heapHeadIndex.getLevel());
		Assert.assertEquals(DEFAULT_LEVEL * 2, heapHeadIndex.getLevelIndex().length);
	}

	/**
	 * Test update to current level is allowed.
	 */
	@Test
	public void testUpdateToCurrentLevel() {
		heapHeadIndex.updateLevel(heapHeadIndex.getLevel());
	}

	/**
	 * Test update to current level is allowed.
	 */
	@Test
	public void testUpdateLevelToLessThanCurrentLevel() {
		int level = heapHeadIndex.getLevel();
		// update level 10 times
		for (int i = 0; i < 10; i++) {
			heapHeadIndex.updateLevel(++level);
		}
		// check update level to values less than current top level
		for (int i = level - 1; i >= 0; i--) {
			heapHeadIndex.updateLevel(i);
			Assert.assertEquals(level, heapHeadIndex.getLevel());
		}
	}

	/**
	 * Test once update more than one level is not allowed.
	 */
	@Test
	public void testOnceUpdateMoreThanOneLevel() {
		try {
			heapHeadIndex.updateLevel(heapHeadIndex.getLevel() + 2);
			Assert.fail("Should have thrown exception");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}
	}

	/**
	 * Test update to negative level is not allowed.
	 */
	@Test
	public void testUpdateToNegativeLevel() {
		try {
			heapHeadIndex.updateLevel(-1);
			Assert.fail("Should throw exception");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}
	}

	/**
	 * Test update to more than max level is not allowed.
	 */
	@Test
	public void testUpdateToMoreThanMaximumAllowed() {
		try {
			heapHeadIndex.updateLevel(MAX_LEVEL + 1);
			Assert.fail("Should throw exception");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}
	}

	@Test
	public void testUpdateNextNode() {
		// test update next node of level 0
		int level = 0;
		long node1 = random.nextLong(Long.MAX_VALUE);
		heapHeadIndex.updateNextNode(level, node1);
		Assert.assertEquals(node1, heapHeadIndex.getNextNode(level));
		// Increase one level and make sure everything still works
		heapHeadIndex.updateLevel(++level);
		long node2 = random.nextLong(Long.MAX_VALUE);
		heapHeadIndex.updateNextNode(level, node2);
		Assert.assertEquals(node2, heapHeadIndex.getNextNode(level));
		Assert.assertEquals(node1, heapHeadIndex.getNextNode(level - 1));
	}

	@Test
	public void testUpdateNextNodeAfterScale() {
		int level = 0;
		for (; level <= DEFAULT_LEVEL; level++) {
			heapHeadIndex.updateLevel(level);
		}
		heapHeadIndex.updateLevel(level);
		long node = random.nextLong(Long.MAX_VALUE);
		heapHeadIndex.updateNextNode(level, node);
		Assert.assertEquals(node, heapHeadIndex.getNextNode(level));
		for (int i = 0; i < level; i++) {
			Assert.assertEquals(NIL_NODE, heapHeadIndex.getNextNode(i));
		}
	}

}
