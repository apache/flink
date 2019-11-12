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

package org.apache.flink.util;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link IterableUtils}.
 */
public class IterableUtilsTest extends TestLogger {

	private final Iterable<Integer> testIterable = Arrays.asList(1, 8, 5, 3, 8);

	@Test
	public void testToStream() {
		Queue<Integer> deque = new ArrayDeque<>();
		testIterable.forEach(deque::add);

		Stream<Integer> stream = IterableUtils.toStream(testIterable);
		assertTrue(stream.allMatch(value -> deque.poll().equals(value)));
	}
}
