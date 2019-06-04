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
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;

import org.junit.Test;

import java.io.IOException;

/**
 * Test for {@link CopyOnWriteStateTable}.
 */
public class CopyOnWriteStateTableTest {

	/**
	 * This tests that serializers used for snapshots are duplicates of the ones used in
	 * processing to avoid race conditions in stateful serializers.
	 */
	@Test
	public void testSerializerDuplicationInSnapshot() throws IOException {

		final TestDuplicateSerializer namespaceSerializer = new TestDuplicateSerializer();
		final TestDuplicateSerializer stateSerializer = new TestDuplicateSerializer();
		final TestDuplicateSerializer keySerializer = new TestDuplicateSerializer();

		RegisteredKeyValueStateBackendMetaInfo<Integer, Integer> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.VALUE,
				"test",
				namespaceSerializer,
				stateSerializer);

		InternalKeyContext<Integer> mockKeyContext = new MockInternalKeyContext<>();
		CopyOnWriteStateTable<Integer, Integer, Integer> table =
			new CopyOnWriteStateTable<>(mockKeyContext, metaInfo, keySerializer);

		table.put(0, 0, 0, 0);
		table.put(1, 0, 0, 1);
		table.put(2, 0, 1, 2);


		final CopyOnWriteStateTableSnapshot<Integer, Integer, Integer> snapshot = table.stateSnapshot();

		final StateSnapshot.StateKeyGroupWriter partitionedSnapshot = snapshot.getKeyGroupWriter();
		namespaceSerializer.disable();
		keySerializer.disable();
		stateSerializer.disable();

		partitionedSnapshot.writeStateInKeyGroup(
			new DataOutputViewStreamWrapper(
				new ByteArrayOutputStreamWithPos(1024)), 0);
	}
}
