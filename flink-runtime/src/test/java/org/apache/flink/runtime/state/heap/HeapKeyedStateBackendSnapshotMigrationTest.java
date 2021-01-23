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
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;

/** Tests backwards compatibility in the serialization format of heap-based KeyedStateBackends. */
public class HeapKeyedStateBackendSnapshotMigrationTest extends HeapStateBackendTestBase {

    @Test
    public void testMapStateMigrationAfterHashMapSerRemoval() throws Exception {
        ClassLoader cl = getClass().getClassLoader();
        URL resource = cl.getResource("heap_keyed_statebackend_1_5_map.snapshot");

        Preconditions.checkNotNull(resource, "Binary snapshot resource not found!");

        final SnapshotResult<KeyedStateHandle> stateHandles;
        try (BufferedInputStream bis =
                new BufferedInputStream((new FileInputStream(resource.getFile())))) {
            stateHandles =
                    InstantiationUtil.deserializeObject(
                            bis, Thread.currentThread().getContextClassLoader());
        }
        final KeyedStateHandle stateHandle = stateHandles.getJobManagerOwnedSnapshot();
        try (final HeapKeyedStateBackend<String> keyedBackend =
                createKeyedBackend(StateObjectCollection.singleton(stateHandle))) {
            final Integer namespace1 = 1;
            final Integer namespace2 = 2;
            final Integer namespace3 = 3;

            final MapStateDescriptor<Long, Long> stateDescr =
                    new MapStateDescriptor<>("my-map-state", Long.class, Long.class);
            stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

            InternalMapState<String, Integer, Long, Long> state =
                    keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            assertEquals(33L, (long) state.get(33L));
            assertEquals(55L, (long) state.get(55L));
            assertEquals(2, getStateSize(state));

            state.setCurrentNamespace(namespace2);
            assertEquals(22L, (long) state.get(22L));
            assertEquals(11L, (long) state.get(11L));
            assertEquals(2, getStateSize(state));

            state.setCurrentNamespace(namespace3);
            assertEquals(44L, (long) state.get(44L));
            assertEquals(1, getStateSize(state));

            keyedBackend.setCurrentKey("def");
            state.setCurrentNamespace(namespace1);
            assertEquals(11L, (long) state.get(11L));
            assertEquals(44L, (long) state.get(44L));
            assertEquals(2, getStateSize(state));

            state.setCurrentNamespace(namespace3);
            assertEquals(22L, (long) state.get(22L));
            assertEquals(55L, (long) state.get(55L));
            assertEquals(33L, (long) state.get(33L));
            assertEquals(3, getStateSize(state));

            keyedBackend.setCurrentKey("jkl");
            state.setCurrentNamespace(namespace1);
            assertEquals(11L, (long) state.get(11L));
            assertEquals(22L, (long) state.get(22L));
            assertEquals(33L, (long) state.get(33L));
            assertEquals(44L, (long) state.get(44L));
            assertEquals(55L, (long) state.get(55L));
            assertEquals(5, getStateSize(state));

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            assertEquals(11L, (long) state.get(11L));
            assertEquals(22L, (long) state.get(22L));
            assertEquals(33L, (long) state.get(33L));
            assertEquals(44L, (long) state.get(44L));
            assertEquals(55L, (long) state.get(55L));
            assertEquals(5, getStateSize(state));

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    keyedBackend.snapshot(
                            1L,
                            1L,
                            new MemCheckpointStreamFactory(4 * 1024 * 1024),
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            snapshot.run();
        }
    }

    private <K, N, UK, UV> int getStateSize(InternalMapState<K, N, UK, UV> mapState)
            throws Exception {
        int i = 0;
        for (Iterator<Map.Entry<UK, UV>> itt = mapState.iterator();
                itt.hasNext();
                i++, itt.next()) {}
        return i;
    }

    //	/**
    //	 * This code was used to create the binary file of the old version's (< 1.5) snapshot used by
    // this test.
    //   * If you need to recreate the binary, you can comment this out and run it.
    //	 */
    //	private void createBinarySnapshotWithMap() throws Exception {
    //
    //		final String pathToWrite = "/PATH/TO/WRITE";
    //
    //		final MapStateDescriptor<Long, Long> stateDescr = new MapStateDescriptor<>("my-map-state",
    // Long.class, Long.class);
    //		stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());
    //
    //		final Integer namespace1 = 1;
    //		final Integer namespace2 = 2;
    //		final Integer namespace3 = 3;
    //
    //		try (final HeapKeyedStateBackend<String> keyedBackend = createKeyedBackend()) {
    //			InternalMapState<String, Integer, Long, Long> state =
    // keyedBackend.createMapState(IntSerializer.INSTANCE, stateDescr);
    //
    //			keyedBackend.setCurrentKey("abc");
    //			state.setCurrentNamespace(namespace1);
    //			state.put(33L, 33L);
    //			state.put(55L, 55L);
    //
    //			state.setCurrentNamespace(namespace2);
    //			state.put(22L, 22L);
    //			state.put(11L, 11L);
    //
    //			state.setCurrentNamespace(namespace3);
    //			state.put(44L, 44L);
    //
    //			keyedBackend.setCurrentKey("def");
    //			state.setCurrentNamespace(namespace1);
    //			state.put(11L, 11L);
    //			state.put(44L, 44L);
    //
    //			state.setCurrentNamespace(namespace3);
    //			state.put(22L, 22L);
    //			state.put(55L, 55L);
    //			state.put(33L, 33L);
    //
    //			keyedBackend.setCurrentKey("jkl");
    //			state.setCurrentNamespace(namespace1);
    //			state.put(11L, 11L);
    //			state.put(22L, 22L);
    //			state.put(33L, 33L);
    //			state.put(44L, 44L);
    //			state.put(55L, 55L);
    //
    //			keyedBackend.setCurrentKey("mno");
    //			state.setCurrentNamespace(namespace3);
    //			state.put(11L, 11L);
    //			state.put(22L, 22L);
    //			state.put(33L, 33L);
    //			state.put(44L, 44L);
    //			state.put(55L, 55L);
    //
    //			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot = keyedBackend.snapshot(
    //					0L,
    //					0L,
    //					new MemCheckpointStreamFactory(4 * 1024 * 1024),
    //					CheckpointOptions.forCheckpointWithDefaultLocation());
    //
    //			snapshot.run();
    //
    //			try (BufferedOutputStream bis = new BufferedOutputStream(new
    // FileOutputStream(pathToWrite))) {
    //				InstantiationUtil.serializeObject(bis, snapshot.get());
    //			}
    //		}
    //	}
}
