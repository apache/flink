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

import org.apache.flink.types.DefaultPair;
import org.apache.flink.types.Pair;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link GroupIterator}.
 */
public class GroupIteratorTest {

	@Test
	public void testGroupIterator() {
		byte[] key = new byte[10];
		byte[] value = new byte[20];
		int length = 100;
		List<Pair<byte[], byte[]>> pairList = new ArrayList<>(length);
		for (int i = 0; i < length; i++) {
			ThreadLocalRandom.current().nextBytes(key);
			ThreadLocalRandom.current().nextBytes(value);
			pairList.add(new DefaultPair<>(key.clone(), value.clone()));
		}
		int startIndex = 0;
		int toIndex = 0;

		Collection<Iterator<Pair<byte[], byte[]>>> groupIterators = new ArrayList<>();
		while (startIndex <= length) {
			toIndex = ThreadLocalRandom.current().nextInt(5) + startIndex;
			if (toIndex > startIndex) {
				groupIterators.add(pairList.subList(startIndex, Math.min(length, toIndex)).iterator());
				startIndex = toIndex;
			} else {
				groupIterators.add(Collections.emptyIterator());
			}
		}
		GroupIterator groupIterator = new GroupIterator(groupIterators);
		int index = 0;
		while (groupIterator.hasNext()) {
			Pair<byte[], byte[]> expectedPair = pairList.get(index);
			Pair<byte[], byte[]> actualPair = groupIterator.next();
			assertEquals(expectedPair.getKey(), actualPair.getKey());
			assertEquals(expectedPair.getValue(), actualPair.getValue());
			index++;
		}
		assertEquals(length, index);

		try {
			groupIterator.next();
			fail();
		} catch (NoSuchElementException e) {
			// ignored
		}

		try {
			groupIterator.remove();
			groupIterator.remove();
			fail();
		} catch (IllegalStateException e) {
			// ignored
		}
	}

}
