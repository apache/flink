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
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

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

	/**
	 * This tests that resource can be released for a successful snapshot.
	 */
	@Test
	public void testReleaseForSuccessfulSnapshot() throws IOException {
		int numberOfKeyGroups = 10;
		CopyOnWriteStateTable<Integer, Integer, Float> table = createStateTableForSnapshotRelease(numberOfKeyGroups);

		ByteArrayOutputStreamWithPos byteArrayOutputStreamWithPos = new ByteArrayOutputStreamWithPos();
		DataOutputView dataOutputView = new DataOutputViewStreamWrapper(byteArrayOutputStreamWithPos);

		CopyOnWriteStateTableSnapshot<Integer, Integer, Float> snapshot = table.stateSnapshot();
		for (int group = 0; group < numberOfKeyGroups; group++) {
			snapshot.writeStateInKeyGroup(dataOutputView, group);
			// resource used by one key group should be released after the snapshot is successful
			Assert.assertTrue(isResourceReleasedForKeyGroup(table, group));
		}
		snapshot.release();
		verifyResourceIsReleasedForAllKeyGroup(table, 1);
	}

	/**
	 * This tests that resource can be released for a failed snapshot.
	 */
	@Test
	public void testReleaseForFailedSnapshot() throws IOException {
		int numberOfKeyGroups = 10;
		CopyOnWriteStateTable<Integer, Integer, Float> table = createStateTableForSnapshotRelease(numberOfKeyGroups);

		ByteArrayOutputStreamWithPos byteArrayOutputStreamWithPos = new ByteArrayOutputStreamWithPos();
		DataOutputView dataOutputView = new DataOutputViewStreamWrapper(byteArrayOutputStreamWithPos);

		CopyOnWriteStateTableSnapshot<Integer, Integer, Float> snapshot = table.stateSnapshot();
		// only snapshot part of key groups to simulate a failed snapshot
		for (int group = 0; group < numberOfKeyGroups / 2; group++) {
			snapshot.writeStateInKeyGroup(dataOutputView, group);
			Assert.assertTrue(isResourceReleasedForKeyGroup(table, group));
		}
		for (int group = numberOfKeyGroups / 2; group < numberOfKeyGroups; group++) {
			Assert.assertFalse(isResourceReleasedForKeyGroup(table, group));
		}
		snapshot.release();
		verifyResourceIsReleasedForAllKeyGroup(table, 2);
	}

	private CopyOnWriteStateTable<Integer, Integer, Float> createStateTableForSnapshotRelease(int numberOfKeyGroups) {
		RegisteredKeyValueStateBackendMetaInfo<Integer, Float> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.VALUE,
				"test",
				IntSerializer.INSTANCE,
				FloatSerializer.INSTANCE);

		MockInternalKeyContext<Integer> mockKeyContext =
			new MockInternalKeyContext<>(0, numberOfKeyGroups - 1, numberOfKeyGroups);
		CopyOnWriteStateTable<Integer, Integer, Float> table =
			new CopyOnWriteStateTable<>(mockKeyContext, metaInfo, IntSerializer.INSTANCE);

		ThreadLocalRandom random = ThreadLocalRandom.current();
		for (int i = 0; i < 1000; i++) {
			mockKeyContext.setCurrentKeyAndKeyGroup(i);
			table.put(random.nextInt(), random.nextFloat());
		}

		return table;
	}

	private void verifyResourceIsReleasedForAllKeyGroup(
		CopyOnWriteStateTable table,
		int snapshotVersion) {
		StateMap[] stateMaps = table.getState();
		for (StateMap map : stateMaps) {
			Assert.assertFalse(((CopyOnWriteStateMap) map).getSnapshotVersions().contains(snapshotVersion));
		}
	}

	private boolean isResourceReleasedForKeyGroup(
		CopyOnWriteStateTable table,
		int keyGroup) {
		CopyOnWriteStateMap stateMap = (CopyOnWriteStateMap) table.getMapForKeyGroup(keyGroup);
		return !stateMap.getSnapshotVersions().contains(1);
	}
}
