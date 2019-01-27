/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RegisteredStateMetaInfo} and its snapshot {@link StateMetaInfoSnapshot}.
 */
public class RegisteredStateMetaInfoTest {

	@Test
	public void testCreateStateMetaInfo() {
		IntSerializer keySerializer = IntSerializer.INSTANCE;
		DoubleSerializer elementSerializer = DoubleSerializer.INSTANCE;
		try {
			RegisteredStateMetaInfo.createKeyedStateMetaInfo(InternalStateType.KEYED_LIST, "list", keySerializer, elementSerializer);
			fail("Should throw IllegalStateException.");
		} catch (IllegalStateException e) {
			// expected exception
		}

		try {
			RegisteredStateMetaInfo.createKeyedStateMetaInfo(InternalStateType.KEYED_MAP, "map", keySerializer, elementSerializer);
			fail("Should throw IllegalStateException.");
		} catch (IllegalStateException e) {
			// expected exception
		}

		try {
			RegisteredStateMetaInfo.createKeyedStateMetaInfo(InternalStateType.KEYED_SORTEDMAP, "sorted-map", keySerializer, elementSerializer);
			fail("Should throw IllegalStateException.");
		} catch (IllegalStateException e) {
			// expected exception
		}

		ListSerializer<Double> listSerializer = new ListSerializer<>(elementSerializer);
		RegisteredStateMetaInfo.createKeyedStateMetaInfo(InternalStateType.KEYED_LIST, "list", keySerializer, listSerializer);

		LongSerializer mapKeySerializer = LongSerializer.INSTANCE;
		StringSerializer mapValueSerializer = StringSerializer.INSTANCE;
		MapSerializer<Long, String> mapSerializer = new MapSerializer<>(mapKeySerializer, mapValueSerializer);
		RegisteredStateMetaInfo.createKeyedStateMetaInfo(InternalStateType.KEYED_MAP, "map", keySerializer, mapSerializer);

		SortedMapSerializer<Long, String> sortedMapSerializer = new SortedMapSerializer<>(new NaturalComparator<>(), mapKeySerializer, mapValueSerializer);
		RegisteredStateMetaInfo.createKeyedStateMetaInfo(InternalStateType.KEYED_SORTEDMAP, "sorted-map", keySerializer, sortedMapSerializer);

	}

	@Test
	public void testStateMetaInfoSnapshot() {
		IntSerializer keySerializer = IntSerializer.INSTANCE;
		ListSerializer<Double> listSerializer = new ListSerializer<>(DoubleSerializer.INSTANCE);

		RegisteredStateMetaInfo listStateMeta = RegisteredStateMetaInfo.createKeyedStateMetaInfo(InternalStateType.KEYED_LIST, "list", keySerializer, listSerializer);
		verifyStateMetaSnapshot(
			listStateMeta,
			listStateMeta.snapshot());

		MapSerializer<Integer, Double> mapSerializer = new MapSerializer<>(IntSerializer.INSTANCE, DoubleSerializer.INSTANCE);
		RegisteredStateMetaInfo mapStateMeta = RegisteredStateMetaInfo.createKeyedStateMetaInfo(InternalStateType.KEYED_MAP, "map", keySerializer, mapSerializer);
		verifyStateMetaSnapshot(
			mapStateMeta,
			mapStateMeta.snapshot());

		LongSerializer nameSpaceSerializer = LongSerializer.INSTANCE;
		RegisteredStateMetaInfo subListStateMeta = RegisteredStateMetaInfo.createSubKeyedStateMetaInfo(InternalStateType.SUBKEYED_LIST, "sub-list", keySerializer, listSerializer, nameSpaceSerializer);
		verifyStateMetaSnapshot(
			subListStateMeta,
			subListStateMeta.snapshot());

		RegisteredStateMetaInfo subMapStateMeta = RegisteredStateMetaInfo.createSubKeyedStateMetaInfo(InternalStateType.SUBKEYED_MAP, "sub-map", keySerializer, mapSerializer, nameSpaceSerializer);
		verifyStateMetaSnapshot(
			subMapStateMeta,
			subMapStateMeta.snapshot());
	}

	private void verifyStateMetaSnapshot(RegisteredStateMetaInfo stateMetaInfo, StateMetaInfoSnapshot stateMetaInfoSnapshot) {
		assertEquals(stateMetaInfo.getStateType(), stateMetaInfoSnapshot.getStateType());
		assertEquals(stateMetaInfo.getName(), stateMetaInfoSnapshot.getName());
		assertEquals(stateMetaInfo.getKeySerializer(), stateMetaInfoSnapshot.getKeySerializer());
		assertEquals(stateMetaInfo.getValueSerializer(), stateMetaInfoSnapshot.getValueSerializer());
		assertEquals(stateMetaInfo.getNamespaceSerializer(), stateMetaInfoSnapshot.getNamespaceSerializer());

		if (stateMetaInfo.getStateType().isKeyedState()) {
			KeyedStateDescriptor keyedStateDescriptor = stateMetaInfoSnapshot.createKeyedStateDescriptor();
			assertEquals(stateMetaInfo.getStateType(), keyedStateDescriptor.getStateType());
			assertEquals(stateMetaInfo.getName(), keyedStateDescriptor.getName());
			assertEquals(stateMetaInfo.getKeySerializer(), keyedStateDescriptor.getKeySerializer());
			assertEquals(stateMetaInfo.getValueSerializer(), keyedStateDescriptor.getValueSerializer());
		} else {
			SubKeyedStateDescriptor subKeyedStateDescriptor = stateMetaInfoSnapshot.createSubKeyedStateDescriptor();
			assertEquals(stateMetaInfo.getStateType(), subKeyedStateDescriptor.getStateType());
			assertEquals(stateMetaInfo.getName(), subKeyedStateDescriptor.getName());
			assertEquals(stateMetaInfo.getKeySerializer(), subKeyedStateDescriptor.getKeySerializer());
			assertEquals(stateMetaInfo.getValueSerializer(), subKeyedStateDescriptor.getValueSerializer());
			assertEquals(stateMetaInfo.getNamespaceSerializer(), subKeyedStateDescriptor.getNamespaceSerializer());
		}
	}
}
