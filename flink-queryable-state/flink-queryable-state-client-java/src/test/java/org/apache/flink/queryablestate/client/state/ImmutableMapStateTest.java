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

package org.apache.flink.queryablestate.client.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link ImmutableMapState}.
 */
public class ImmutableMapStateTest {

	private final MapStateDescriptor<Long, Long> mapStateDesc =
			new MapStateDescriptor<>(
					"test",
					BasicTypeInfo.LONG_TYPE_INFO,
					BasicTypeInfo.LONG_TYPE_INFO);

	private MapState<Long, Long> mapState;

	@Before
	public void setUp() throws Exception {
		if (!mapStateDesc.isSerializerInitialized()) {
			mapStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());
		}

		Map<Long, Long> initMap = new HashMap<>();
		initMap.put(1L, 5L);
		initMap.put(2L, 5L);

		byte[] initSer = KvStateSerializer.serializeMap(
				initMap.entrySet(),
				BasicTypeInfo.LONG_TYPE_INFO.createSerializer(new ExecutionConfig()),
				BasicTypeInfo.LONG_TYPE_INFO.createSerializer(new ExecutionConfig()));

		mapState = ImmutableMapState.createState(mapStateDesc, initSer);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPut() throws Exception {
		assertTrue(mapState.contains(1L));
		long value = mapState.get(1L);
		assertEquals(5L, value);

		assertTrue(mapState.contains(2L));
		value = mapState.get(2L);
		assertEquals(5L, value);

		mapState.put(2L, 54L);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testPutAll() throws Exception {
		assertTrue(mapState.contains(1L));
		long value = mapState.get(1L);
		assertEquals(5L, value);

		assertTrue(mapState.contains(2L));
		value = mapState.get(2L);
		assertEquals(5L, value);

		Map<Long, Long> nMap = new HashMap<>();
		nMap.put(1L, 7L);
		nMap.put(2L, 7L);

		mapState.putAll(nMap);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUpdate() throws Exception {
		assertTrue(mapState.contains(1L));
		long value = mapState.get(1L);
		assertEquals(5L, value);

		assertTrue(mapState.contains(2L));
		value = mapState.get(2L);
		assertEquals(5L, value);

		mapState.put(2L, 54L);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testIterator() throws Exception {
		assertTrue(mapState.contains(1L));
		long value = mapState.get(1L);
		assertEquals(5L, value);

		assertTrue(mapState.contains(2L));
		value = mapState.get(2L);
		assertEquals(5L, value);

		Iterator<Map.Entry<Long, Long>> iterator = mapState.iterator();
		while (iterator.hasNext()) {
			iterator.remove();
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testIterable() throws Exception {
		assertTrue(mapState.contains(1L));
		long value = mapState.get(1L);
		assertEquals(5L, value);

		assertTrue(mapState.contains(2L));
		value = mapState.get(2L);
		assertEquals(5L, value);

		Iterable<Map.Entry<Long, Long>> iterable = mapState.entries();
		Iterator<Map.Entry<Long, Long>> iterator = iterable.iterator();
		while (iterator.hasNext()) {
			assertEquals(5L, (long) iterator.next().getValue());
			iterator.remove();
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testKeys() throws Exception {
		assertTrue(mapState.contains(1L));
		long value = mapState.get(1L);
		assertEquals(5L, value);

		assertTrue(mapState.contains(2L));
		value = mapState.get(2L);
		assertEquals(5L, value);

		Iterator<Long> iterator = mapState.keys().iterator();
		while (iterator.hasNext()) {
			iterator.remove();
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testValues() throws Exception {
		assertTrue(mapState.contains(1L));
		long value = mapState.get(1L);
		assertEquals(5L, value);

		assertTrue(mapState.contains(2L));
		value = mapState.get(2L);
		assertEquals(5L, value);

		Iterator<Long> iterator = mapState.values().iterator();
		while (iterator.hasNext()) {
			iterator.remove();
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testClear() throws Exception {
		assertTrue(mapState.contains(1L));
		long value = mapState.get(1L);
		assertEquals(5L, value);

		assertTrue(mapState.contains(2L));
		value = mapState.get(2L);
		assertEquals(5L, value);

		mapState.clear();
	}

	@Test
	public void testIsEmpty() throws Exception {
		assertFalse(mapState.isEmpty());
	}
}
