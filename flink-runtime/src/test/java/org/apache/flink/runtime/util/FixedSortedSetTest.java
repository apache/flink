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

package org.apache.flink.runtime.util;

import org.junit.Test;

import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FixedSortedSetTest {

	@Test
	public void testAdd() {

		final FixedSortedSet<Long> list = new FixedSortedSet<>(2, Comparator.reverseOrder());

		list.add(2L);

		assertTrue(list.contains(2L));
		assertEquals(1, list.size());

		list.add(1L);

		assertTrue(list.contains(1L));
		assertEquals(2, list.size());

		list.add(3L);

		assertTrue(list.contains(3L));
		assertFalse(list.contains(1L));
		assertEquals(2, list.size());

		list.add(1L);

		assertTrue(list.contains(2L));
		assertTrue(list.contains(3L));
		assertFalse(list.contains(1L));
		assertEquals(2, list.size());
	}

}
