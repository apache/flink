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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class StateTableSnapshotCompatibilityTest {

	/**
	 * This test ensures that different implementations of {@link StateTable} are compatible in their serialization
	 * format.
	 */
	@Test
	public void checkCompatibleSerializationFormats() throws IOException {
		final Random r = new Random(42);
		RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.UNKNOWN,
				"test",
				IntSerializer.INSTANCE,
				new ArrayListSerializer<>(IntSerializer.INSTANCE));

		final CopyOnWriteStateTableTest.MockInternalKeyContext<Integer> keyContext =
			new CopyOnWriteStateTableTest.MockInternalKeyContext<>(IntSerializer.INSTANCE);

		CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> cowStateTable =
			new CopyOnWriteStateTable<>(keyContext, metaInfo);

		for (int i = 0; i < 100; ++i) {
			ArrayList<Integer> list = new ArrayList<>(5);
			int end = r.nextInt(5);
			for (int j = 0; j < end; ++j) {
				list.add(r.nextInt(100));
			}

			cowStateTable.put(r.nextInt(10), r.nextInt(2), list);
		}

		StateSnapshot snapshot = cowStateTable.stateSnapshot();

		final NestedMapsStateTable<Integer, Integer, ArrayList<Integer>> nestedMapsStateTable =
			new NestedMapsStateTable<>(keyContext, metaInfo);

		restoreStateTableFromSnapshot(nestedMapsStateTable, snapshot, keyContext.getKeyGroupRange());
		snapshot.release();


		Assert.assertEquals(cowStateTable.size(), nestedMapsStateTable.size());
		for (StateEntry<Integer, Integer, ArrayList<Integer>> entry : cowStateTable) {
			Assert.assertEquals(entry.getState(), nestedMapsStateTable.get(entry.getKey(), entry.getNamespace()));
		}

		snapshot = nestedMapsStateTable.stateSnapshot();
		cowStateTable = new CopyOnWriteStateTable<>(keyContext, metaInfo);

		restoreStateTableFromSnapshot(cowStateTable, snapshot, keyContext.getKeyGroupRange());
		snapshot.release();

		Assert.assertEquals(nestedMapsStateTable.size(), cowStateTable.size());
		for (StateEntry<Integer, Integer, ArrayList<Integer>> entry : cowStateTable) {
			Assert.assertEquals(nestedMapsStateTable.get(entry.getKey(), entry.getNamespace()), entry.getState());
		}
	}

	private static <K, N, S> void restoreStateTableFromSnapshot(
		StateTable<K, N, S> stateTable,
		StateSnapshot snapshot,
		KeyGroupRange keyGroupRange) throws IOException {

		final ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos(1024 * 1024);
		final DataOutputViewStreamWrapper dov = new DataOutputViewStreamWrapper(out);
		final StateSnapshot.StateKeyGroupWriter keyGroupPartitionedSnapshot = snapshot.getKeyGroupWriter();
		for (Integer keyGroup : keyGroupRange) {
			keyGroupPartitionedSnapshot.writeStateInKeyGroup(dov, keyGroup);
		}

		final ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(out.getBuf());
		final DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(in);

		final StateSnapshotKeyGroupReader keyGroupReader =
			StateTableByKeyGroupReaders.readerForVersion(stateTable, KeyedBackendSerializationProxy.VERSION);

		for (Integer keyGroup : keyGroupRange) {
			keyGroupReader.readMappingsInKeyGroup(div, keyGroup);
		}
	}
}
