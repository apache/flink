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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Collections;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class HeapKeyedStateBackendSnapshotBackwardsCompatibilityTest {

	/**
	 * This test takes a snapshot that was created with Flink 1.2 and tries to restore it in Flink 1.3 to check
	 * the backwards compatibility of the serialization format of {@link StateTable}s.
	 */
	@Test
	public void testHeapKeyedStateBackend1_2To1_3BackwardsCompatibility() throws Exception {

		ClassLoader cl = getClass().getClassLoader();
		URL resource = cl.getResource("heap_keyed_statebackend_1_2.snapshot");

		Preconditions.checkNotNull(resource, "Binary snapshot resource not found!");

		final Integer namespace1 = 1;
		final Integer namespace2 = 2;
		final Integer namespace3 = 3;

		try (final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend()) {
			final KeyGroupsStateHandle stateHandle;
			try (BufferedInputStream bis = new BufferedInputStream((new FileInputStream(resource.getFile())))) {
				stateHandle = InstantiationUtil.deserializeObject(bis, Thread.currentThread().getContextClassLoader());
			}
			keyedBackend.restore(Collections.singleton(stateHandle));
			final ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("my-state", Long.class);
			stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

			InternalListState<Integer, Long> state = keyedBackend.createListState(IntSerializer.INSTANCE, stateDescr);

			assertEquals(7, keyedBackend.numStateEntries());

			keyedBackend.setCurrentKey("abc");
			state.setCurrentNamespace(namespace1);
			assertEquals(asList(33L, 55L), state.get());
			state.setCurrentNamespace(namespace2);
			assertEquals(asList(22L, 11L), state.get());
			state.setCurrentNamespace(namespace3);
			assertEquals(Collections.singletonList(44L), state.get());

			keyedBackend.setCurrentKey("def");
			state.setCurrentNamespace(namespace1);
			assertEquals(asList(11L, 44L), state.get());

			state.setCurrentNamespace(namespace3);
			assertEquals(asList(22L, 55L, 33L), state.get());

			keyedBackend.setCurrentKey("jkl");
			state.setCurrentNamespace(namespace1);
			assertEquals(asList(11L, 22L, 33L, 44L, 55L), state.get());

			keyedBackend.setCurrentKey("mno");
			state.setCurrentNamespace(namespace3);
			assertEquals(asList(11L, 22L, 33L, 44L, 55L), state.get());
		}
	}

	private static HeapKeyedStateBackend<String> createKeyedBackend() throws Exception {
		return new HeapKeyedStateBackend<>(
				mock(TaskKvStateRegistry.class),
				StringSerializer.INSTANCE,
				HeapListStateTest.class.getClassLoader(),
				16,
				new KeyGroupRange(0, 15),
				(System.currentTimeMillis() & 1) == 1);
	}
}