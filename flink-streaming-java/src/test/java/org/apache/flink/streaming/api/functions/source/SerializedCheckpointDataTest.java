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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link SerializedCheckpointData}.
 */
public class SerializedCheckpointDataTest {
	@Test
	public void testCombiningCheckpointData_Empty() {
		ArrayDeque<Tuple2<Long, Set<String>>> actual = SerializedCheckpointData.combine(emptyList());
		assertThat(actual.size(), is(0));
	}

	@Test
	public void testCombiningCheckpointData_OneElement() {
		Map<Long, Set<String>> checkpointData = new HashMap<Long, Set<String>>() {{
			put(1L, asSet("firstId", "secondId"));
		}};
		ArrayDeque<Tuple2<Long, Set<String>>> actual = SerializedCheckpointData.combine(singletonList(checkpointData));
		assertThat(actual.size(), is(1));
		assertThat(actual.getFirst(), equalTo(Tuple2.of(1L, asSet("firstId", "secondId"))));
	}

	@Test
	public void testCombiningCheckpointData_MultipleData() {
		Map<Long, Set<String>> firstCheckpointData = new HashMap<Long, Set<String>>() {{
			put(1L, asSet("firstId"));
			put(2L, asSet("secondId"));
		}};
		Map<Long, Set<String>> secondCheckpointData = new HashMap<Long, Set<String>>() {{
			put(2L, asSet("thirdId"));
			put(3L, asSet("forthId"));
		}};
		ArrayDeque<Tuple2<Long, Set<String>>> actual =
			SerializedCheckpointData.combine(asList(firstCheckpointData, secondCheckpointData));
		assertThat(actual.size(), is(3));
		assertThat(actual.pop(), equalTo(Tuple2.of(1L, asSet("firstId"))));
		assertThat(actual.pop(), equalTo(Tuple2.of(2L, asSet("secondId", "thirdId"))));
		assertThat(actual.pop(), equalTo(Tuple2.of(3L, asSet("forthId"))));
	}

	private Set<String> asSet(String...ids) {
		return new TreeSet<>(Arrays.asList(ids));
	}
}
