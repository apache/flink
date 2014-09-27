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

package org.apache.flink.streaming.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

public class InternalStateTest {

	@Test
	public void MutableTableStateTest() {
		TableState<String, String> state = new TableState<String, String>();
		state.put("abc", "hello");
		state.put("test", "world");
		state.put("state", "mutable");
		state.put("streaming", "persist");
		String s = state.get("streaming");
		assertEquals("persist", s);

		s = state.get("null");

		assertNull(s);

		TableStateIterator<String, String> iterator = state.getIterator();

		Set<Tuple2<String, String>> expected = new HashSet<Tuple2<String, String>>();
		expected.add(new Tuple2<String, String>("abc", "hello"));
		expected.add(new Tuple2<String, String>("test", "world"));
		expected.add(new Tuple2<String, String>("state", "mutable"));
		expected.add(new Tuple2<String, String>("streaming", "persist"));

		Set<Tuple2<String, String>> actual = new HashSet<Tuple2<String, String>>();

		while (iterator.hasNext()) {
			actual.add(iterator.next());
		}
		assertEquals(expected, actual);
	}
}
