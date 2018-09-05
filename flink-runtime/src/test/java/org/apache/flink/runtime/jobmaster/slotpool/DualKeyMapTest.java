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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link DualKeyMap}.
 */
public class DualKeyMapTest extends TestLogger {

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

		final DualKeyMap<Integer, Integer, String> dualKeyMap = new DualKeyMap<>(capacity);

		for (Tuple2<Integer, Integer> key : keys) {
			dualKeyMap.put(key.f0, key.f1, "foobar");
		}

		assertThat(dualKeyMap.keySetA(), Matchers.equalTo(keys.stream().map(t -> t.f0).collect(Collectors.toSet())));
		assertThat(dualKeyMap.keySetB(), Matchers.equalTo(keys.stream().map(t -> t.f1).collect(Collectors.toSet())));
	}
}
