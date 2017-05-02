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

package org.apache.flink.python.api.streaming.data;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Tests for the SingleElementPushBackIterator.
 */
public class SingleElementPushBackIteratorTest {

	@Test
	public void testPushBackIterator() {
		Collection<Integer> init = new ArrayList<>();
		init.add(1);
		init.add(2);
		init.add(4);
		init.add(5);
		SingleElementPushBackIterator<Integer> iterator = new SingleElementPushBackIterator<>(init.iterator());

		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(1, (int) iterator.next());

		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(2, (int) iterator.next());

		Assert.assertTrue(iterator.hasNext());
		iterator.pushBack(3);
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(3, (int) iterator.next());

		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(4, (int) iterator.next());

		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(5, (int) iterator.next());

		Assert.assertFalse(iterator.hasNext());
		iterator.pushBack(6);
		Assert.assertTrue(iterator.hasNext());
		Assert.assertEquals(6, (int) iterator.next());

		Assert.assertFalse(iterator.hasNext());
	}

	@Test
	public void testSingleElementLimitation() {
		Collection<Integer> init = Collections.emptyList();
		SingleElementPushBackIterator<Integer> iterator = new SingleElementPushBackIterator<>(init.iterator());
		Assert.assertFalse(iterator.hasNext());
		iterator.pushBack(1);
		try {
			iterator.pushBack(2);
			Assert.fail("Multiple elements could be pushed back.");
		} catch (IllegalStateException ignored) {
			// expected
		}
	}
}
