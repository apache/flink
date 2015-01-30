/*
 *
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
 *
 */

package org.apache.flink.streaming.connectors.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;

import org.junit.Ignore;
import org.junit.Test;

public class DBStateTest {
	
	public void stateTest(DBState<String, Integer> state) {
		state.put("k1", 1);
		assertEquals(Integer.valueOf(1), state.get("k1"));

		state.put("k2", 2);
		state.put("k3", 3);
		assertEquals(Integer.valueOf(2), state.get("k2"));
		assertEquals(Integer.valueOf(3), state.get("k3"));
		state.remove("k2");
		
		try {
			state.get("k2");
			fail();
		} catch (Exception e) {
		}
	}
	
	private void iteratorTest(DBStateWithIterator<String, Integer> state) {
		HashMap<String, Integer> expected = new HashMap<String, Integer>();
		HashMap<String, Integer> result = new HashMap<String, Integer>();
		
		state.put("10", 10);
		state.put("20", 20);
		state.put("30", 30);

		expected.put("10", 10);
		expected.put("20", 20);
		expected.put("30", 30);
		
		DBStateIterator<String, Integer> iterator = state.getIterator();
		while (iterator.hasNext()) {
			String key = iterator.getNextKey();
			Integer value = iterator.getNextValue();
			result.put(key, value);
			iterator.next();
		}
		state.close();
		
		assertEquals(expected, result);
	}
	
	// TODO
	@Ignore("Creates files with no licenses")
	@Test
	public void levelDBTest() {
		LevelDBState<String, Integer> state = new LevelDBState<String, Integer>("test");
		stateTest(state);
		state.close();
		
		state = new LevelDBState<String, Integer>("test");
		iteratorTest(state);
		state.close();
	}
	
	@Ignore("Needs running Memcached")
	@Test
	public void memcachedTest() {
		MemcachedState<Integer> state = new MemcachedState<Integer>();
		stateTest(state);
		state.close();
	}

	@Ignore("Needs running Redis")
	@Test
	public void redisTest() {
		RedisState<String, Integer> state = new RedisState<String, Integer>();
		stateTest(state);
		state.close();
		
		state = new RedisState<String, Integer>();
		iteratorTest(state);
		state.close();
	}
}
