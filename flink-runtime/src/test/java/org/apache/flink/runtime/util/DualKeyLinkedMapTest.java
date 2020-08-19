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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link DualKeyLinkedMap}.
 */
public class DualKeyLinkedMapTest extends TestLogger {

	@Test
	public void testKeySets() {
		final Random random = new Random();
		final int capacity = 10;
		final Set<Tuple2<Integer, Integer>> keys = new HashSet<>(capacity);

		for (int i = 0; i < capacity; i++) {
			int keyA = random.nextInt();
			int keyB = random.nextInt();
			keys.add(Tuple2.of(keyA, keyB));
		}

		final DualKeyLinkedMap<Integer, Integer, String> dualKeyMap = new DualKeyLinkedMap<>(capacity);

		for (Tuple2<Integer, Integer> key : keys) {
			dualKeyMap.put(key.f0, key.f1, "foobar");
		}

		assertThat(dualKeyMap.keySetA(), Matchers.equalTo(keys.stream().map(t -> t.f0).collect(Collectors.toSet())));
		assertThat(dualKeyMap.keySetB(), Matchers.equalTo(keys.stream().map(t -> t.f1).collect(Collectors.toSet())));
	}

	@Test
	public void ensuresOneToOneMappingBetweenKeysSamePrimaryKey() {
		final DualKeyLinkedMap<Integer, Integer, String> map = new DualKeyLinkedMap<>(2);

		final String secondValue = "barfoo";
		map.put(1, 1, "foobar");
		map.put(1, 2, secondValue);

		assertThat(map.getValueByKeyB(1), nullValue());
		assertThat(map.getValueByKeyA(1), is(secondValue));
		assertThat(map.getValueByKeyB(2), is(secondValue));
	}

	@Test
	public void ensuresOneToOneMappingBetweenKeysSameSecondaryKey() {
		final DualKeyLinkedMap<Integer, Integer, String> map = new DualKeyLinkedMap<>(2);

		final String secondValue = "barfoo";
		map.put(1, 1, "foobar");
		map.put(2, 1, secondValue);

		assertThat(map.getValueByKeyA(1), nullValue());
		assertThat(map.getValueByKeyB(1), is(secondValue));
		assertThat(map.getValueByKeyA(2), is(secondValue));
	}

	@Test
	public void testPrimaryKeyOrderIsNotAffectedIfReInsertedWithSameSecondaryKey() {
		final DualKeyLinkedMap<Integer, Integer, String> map = new DualKeyLinkedMap<>(2);

		final String value1 = "1";
		map.put(1, 1, value1);
		final String value2 = "2";
		map.put(2, 2, value2);

		final String value3 = "3";
		map.put(1, 1, value3);
		assertThat(map.keySetA().iterator().next(), is(1));
		assertThat(map.values().iterator().next(), is(value3));
	}

	@Test
	public void testPrimaryKeyOrderIsNotAffectedIfReInsertedWithDifferentSecondaryKey() {
		final DualKeyLinkedMap<Integer, Integer, String> map = new DualKeyLinkedMap<>(2);

		final String value1 = "1";
		map.put(1, 1, value1);
		final String value2 = "2";
		map.put(2, 2, value2);

		final String value3 = "3";
		map.put(1, 3, value3);
		assertThat(map.keySetA().iterator().next(), is(1));
		assertThat(map.values().iterator().next(), is(value3));
	}
}
