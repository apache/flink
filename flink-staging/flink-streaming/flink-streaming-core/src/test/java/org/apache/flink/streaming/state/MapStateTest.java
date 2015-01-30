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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.state.checkpoint.MapCheckpoint;
import org.apache.flink.streaming.state.checkpoint.StateCheckpoint;
import org.junit.Test;

public class MapStateTest {

	@Test
	public void testMapState() {

		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("a", 1);
		map.put("b", 2);
		map.put("c", 3);
		map.remove("a");

		MapState<String, Integer> mapState = new MapState<String, Integer>();
		mapState.put("a", 1);
		mapState.put("b", 2);
		mapState.put("c", 3);

		assertEquals(1, (int) mapState.remove("a"));
		assertEquals(null, mapState.remove("a"));
		assertEquals(2, mapState.size());
		assertEquals(map, mapState.state);
		assertEquals(map.entrySet(), mapState.entrySet());
		assertTrue(mapState.containsKey("b"));
		assertFalse(mapState.containsKey("a"));

		assertEquals(2, mapState.updatedItems.size());
		assertEquals(1, mapState.removedItems.size());

		Map<String, Integer> map2 = new HashMap<String, Integer>();
		map2.put("a", 0);
		map2.put("e", -1);

		mapState.putAll(map2);

		assertEquals(4, mapState.updatedItems.size());
		assertEquals(0, mapState.removedItems.size());

		mapState.clear();
		assertEquals(new HashMap<String, Integer>(), mapState.state);
		assertTrue(mapState.clear);
		assertEquals(0, mapState.updatedItems.size());
		assertEquals(0, mapState.removedItems.size());

		mapState.putAll(map);
		assertEquals(map.keySet(), mapState.updatedItems);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testMapStateCheckpointing() {

		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("a", 1);
		map.put("b", 2);
		map.put("c", 3);

		MapState<String, Integer> mapState = new MapState<String, Integer>();
		mapState.putAll(map);

		StateCheckpoint<Map<String, Integer>> mcp = mapState.checkpoint();
		assertEquals(map, mcp.getCheckpointedState());

		Map<String, Integer> map2 = new HashMap<String, Integer>();
		map2.put("a", 0);
		map2.put("e", -1);

		mapState.put("a", 0);
		mapState.put("e", -1);
		mapState.remove("b");
		StateCheckpoint<Map<String, Integer>> mcp2 = new MapCheckpoint<String, Integer>(mapState);
		assertEquals(map2, mcp2.getCheckpointedState());
		mcp.update(mcp2);
		assertEquals(mapState.state, mcp.getCheckpointedState());

		mapState.clear();
		mapState.put("a", 1);
		mapState.put("a", 2);
		mapState.put("b", -3);
		mapState.put("c", 0);
		mapState.remove("b");
		mcp.update(mapState.checkpoint());
		assertEquals(mapState.state, mcp.getCheckpointedState());

		MapState<String, Integer> mapState2 = (MapState<String, Integer>) new MapState<String, Integer>()
				.restore(mcp);

		assertTrue(mapState2.stateEquals(mapState));
		mapState2.reBuild(mapState2.repartition(10));
		assertTrue(mapState2.stateEquals(mapState));

		MapState<Integer, Integer> mapState3 = new MapState<Integer, Integer>();
		mapState3.put(1, 1);
		mapState3.put(2, 1);

		mapState3.reBuild(mapState3.repartition(2)[0]);
		assertTrue(mapState3.containsKey(2));
		assertFalse(mapState3.containsKey(1));

	}
}
