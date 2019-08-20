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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.state.heap.SkipListUtils.DEFAULT_LEVEL;
import static org.apache.flink.runtime.state.heap.SkipListUtils.MAX_LEVEL;
import static org.apache.flink.runtime.state.heap.SkipListUtils.NIL_NODE;

/**
 * Tests for {@link OnHeapLevelIndexHeader}.
 */
public class OnHeapLevelIndexHeaderTest {

	@Test
	public void testNormal() {
		OnHeapLevelIndexHeader heapHeadIndex = new OnHeapLevelIndexHeader();
		ThreadLocalRandom random = ThreadLocalRandom.current();

		Assert.assertEquals(1, heapHeadIndex.getLevel());
		Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelIndex().length);
		for (long node : heapHeadIndex.getLevelIndex()) {
			Assert.assertEquals(NIL_NODE, node);
		}
		for (int level = 0; level <= heapHeadIndex.getLevel(); level++) {
			Assert.assertEquals(NIL_NODE, heapHeadIndex.getNextNode(level));
		}

		// 1. update level 0
		long nextNode = random.nextLong(Long.MAX_VALUE);
		heapHeadIndex.updateNextNode(0, nextNode);
		Assert.assertEquals(nextNode, heapHeadIndex.getNextNode(0));

		// 2. update level 1 multiple times
		heapHeadIndex.updateLevel(1);
		Assert.assertEquals(1, heapHeadIndex.getLevel());
		Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelIndex().length);

		long node1 = random.nextLong(Long.MAX_VALUE);
		heapHeadIndex.updateNextNode(1, node1);
		Assert.assertEquals(nextNode, heapHeadIndex.getNextNode(0));
		for (int i = 1; i <= heapHeadIndex.getLevel(); i++) {
			if (i == 1) {
				Assert.assertEquals(node1, heapHeadIndex.getNextNode(i));
			} else {
				Assert.assertEquals(NIL_NODE, heapHeadIndex.getNextNode(i));
			}
		}

		long node2 = random.nextLong(Long.MAX_VALUE);
		heapHeadIndex.updateNextNode(1, node2);
		Assert.assertEquals(1, heapHeadIndex.getLevel());
		Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelIndex().length);
		Assert.assertEquals(nextNode, heapHeadIndex.getNextNode(0));
		for (int i = 1; i <= heapHeadIndex.getLevel(); i++) {
			if (i == 1) {
				Assert.assertEquals(node2, heapHeadIndex.getNextNode(i));
			} else {
				Assert.assertEquals(NIL_NODE, heapHeadIndex.getNextNode(i));
			}
		}

		long node3 = random.nextLong(Long.MAX_VALUE);
		heapHeadIndex.updateNextNode(1, node3);
		Assert.assertEquals(1, heapHeadIndex.getLevel());
		Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelIndex().length);
		Assert.assertEquals(nextNode, heapHeadIndex.getNextNode(0));
		for (int i = 1; i <= heapHeadIndex.getLevel(); i++) {
			if (i == 1) {
				Assert.assertEquals(node3, heapHeadIndex.getNextNode(i));
			} else {
				Assert.assertEquals(NIL_NODE, heapHeadIndex.getNextNode(i));
			}
		}

		// 3. try to grow by one level
		long node4 = random.nextLong(Long.MAX_VALUE);
		heapHeadIndex.updateLevel(2);
		heapHeadIndex.updateNextNode(2, node4);
		Assert.assertEquals(2, heapHeadIndex.getLevel());
		Assert.assertEquals(DEFAULT_LEVEL, heapHeadIndex.getLevelIndex().length);
		Assert.assertEquals(nextNode, heapHeadIndex.getNextNode(0));
		for (int i = 1; i <= heapHeadIndex.getLevel(); i++) {
			if (i == 1) {
				Assert.assertEquals(node3, heapHeadIndex.getNextNode(i));
			} else if (i == 2) {
				Assert.assertEquals(node4, heapHeadIndex.getNextNode(i));
			} else {
				Assert.assertEquals(NIL_NODE, heapHeadIndex.getNextNode(i));
			}
		}

		// 4. update multiple levels
		for (int i = 1; i < 34; i++) {
			heapHeadIndex.updateLevel(i);
		}
		Assert.assertEquals(33, heapHeadIndex.getLevel());
		Assert.assertEquals(DEFAULT_LEVEL * 2, heapHeadIndex.getLevelIndex().length);
		Assert.assertEquals(nextNode, heapHeadIndex.getNextNode(0));
		for (int i = 1; i <= heapHeadIndex.getLevel(); i++) {
			if (i == 1) {
				Assert.assertEquals(node3, heapHeadIndex.getNextNode(i));
			} else if (i == 2) {
				Assert.assertEquals(node4, heapHeadIndex.getNextNode(i));
			} else {
				Assert.assertEquals(NIL_NODE, heapHeadIndex.getNextNode(i));
			}
		}
	}

	@Test
	public void testException() {
		OnHeapLevelIndexHeader heapHeadIndex = new OnHeapLevelIndexHeader();

		// test that update level by level
		try {
			heapHeadIndex.updateLevel(3);
			Assert.fail("Should throw exception");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		// test that update a negative level
		try {
			heapHeadIndex.updateLevel(-1);
			Assert.fail("Should throw exception");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}

		// test that update max level
		try {
			heapHeadIndex.updateLevel(MAX_LEVEL + 1);
			Assert.fail("Should throw exception");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}
	}
}
