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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TransactionalIdsGenerator}.
 */
public class TransactionalIdsGeneratorTest {
	private static final int POOL_SIZE = 3;
	private static final int SAFE_SCALE_DOWN_FACTOR = 3;
	private static final int SUBTASKS_COUNT = 5;

	@Test
	public void testGenerateIdsToUse() {
		TransactionalIdsGenerator generator = new TransactionalIdsGenerator("test", 2, SUBTASKS_COUNT, POOL_SIZE, SAFE_SCALE_DOWN_FACTOR);

		assertEquals(
			new HashSet<>(Arrays.asList("test-42", "test-43", "test-44")),
			generator.generateIdsToUse(36));
	}

	/**
	 * Task name may contain percent sign which should not impact the formatter.
	 */
	@Test
	public void testTaskNameMayContainPercentSign() {
		TransactionalIdsGenerator generator = new TransactionalIdsGenerator("%pattern%", 2, SUBTASKS_COUNT, POOL_SIZE, SAFE_SCALE_DOWN_FACTOR);

		assertEquals(
			new HashSet<>(Arrays.asList("%pattern%-42", "%pattern%-43", "%pattern%-44")),
			generator.generateIdsToUse(36));
	}

	/**
	 * Ids to abort and to use should never clash between subtasks.
	 */
	@Test
	public void testGeneratedIdsDoNotClash() {
		List<Set<String>> idsToAbort = new ArrayList<>();
		List<Set<String>> idsToUse = new ArrayList<>();

		for (int subtask = 0; subtask < SUBTASKS_COUNT; subtask++) {
			TransactionalIdsGenerator generator = new TransactionalIdsGenerator("test", subtask, SUBTASKS_COUNT, POOL_SIZE, SAFE_SCALE_DOWN_FACTOR);
			idsToUse.add(generator.generateIdsToUse(0));
			idsToAbort.add(generator.generateIdsToAbort());
		}

		for (int subtask1 = 0; subtask1 < SUBTASKS_COUNT; subtask1++) {
			for (int subtask2 = 0; subtask2 < SUBTASKS_COUNT; subtask2++) {
				if (subtask2 == subtask1) {
					continue;
				}
				assertDisjoint(idsToAbort.get(subtask2), idsToAbort.get(subtask1));
				assertDisjoint(idsToUse.get(subtask2), idsToUse.get(subtask1));
				assertDisjoint(idsToAbort.get(subtask2), idsToUse.get(subtask1));
			}
		}
	}

	private <T> void assertDisjoint(Set<T> first, Set<T> second) {
		HashSet<T> actual = new HashSet<>(first);
		actual.retainAll(second);
		assertEquals(Collections.emptySet(), actual);
	}
}
