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
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
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

/**
 * Tests backwards compatibility in the serialization format of heap-based KeyedStateBackends.
 */
public class HeapKeyedStateBackendSnapshotMigrationTest extends HeapStateBackendTestBase {

	/**
	 * [FLINK-5979]
	 *
	 * This test takes a snapshot that was created with Flink 1.2 and tries to restore it in master to check
	 * the backwards compatibility of the serialization format of {@link StateTable}s.
	 */
	@Test
	public void testRestore1_2ToMaster() throws Exception {

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
			keyedBackend.restore(StateObjectCollection.singleton(stateHandle));
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

//	/**
//	 * This code was used to create the binary file of the old version's snapshot used by this test. If you need to
//	 * recreate the binary, you can comment this out and run it.
//	 */
//	private void createBinarySnapshot() throws Exception {
//
//		final String pathToWrite = "/PATH/TO/WRITE";
//
//		final ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("my-state", Long.class);
//		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());
//
//		final Integer namespace1 = 1;
//		final Integer namespace2 = 2;
//		final Integer namespace3 = 3;
//
//		final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend();
//
//		try {
//			InternalListState<Integer, Long> state = keyedBackend.createListState(IntSerializer.INSTANCE, stateDescr);
//
//			keyedBackend.setCurrentKey("abc");
//			state.setCurrentNamespace(namespace1);
//			state.add(33L);
//			state.add(55L);
//
//			state.setCurrentNamespace(namespace2);
//			state.add(22L);
//			state.add(11L);
//
//			state.setCurrentNamespace(namespace3);
//			state.add(44L);
//
//			keyedBackend.setCurrentKey("def");
//			state.setCurrentNamespace(namespace1);
//			state.add(11L);
//			state.add(44L);
//
//			state.setCurrentNamespace(namespace3);
//			state.add(22L);
//			state.add(55L);
//			state.add(33L);
//
//			keyedBackend.setCurrentKey("jkl");
//			state.setCurrentNamespace(namespace1);
//			state.add(11L);
//			state.add(22L);
//			state.add(33L);
//			state.add(44L);
//			state.add(55L);
//
//			keyedBackend.setCurrentKey("mno");
//			state.setCurrentNamespace(namespace3);
//			state.add(11L);
//			state.add(22L);
//			state.add(33L);
//			state.add(44L);
//			state.add(55L);
//			RunnableFuture<KeyGroupsStateHandle> snapshot = keyedBackend.snapshot(
//					0L,
//					0L,
//					new MemCheckpointStreamFactory(4 * 1024 * 1024),
//					CheckpointOptions.forCheckpointWithDefaultLocation());
//
//			snapshot.run();
//
//			try (BufferedOutputStream bis = new BufferedOutputStream(new FileOutputStream(pathToWrite))) {
//				InstantiationUtil.serializeObject(bis, snapshot.get());
//			}
//
//		} finally {
//			keyedBackend.close();
//			keyedBackend.dispose();
//		}
//	}
}
