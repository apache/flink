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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class CopyOnWriteStateTableTest extends TestLogger {

	/**
	 * Testing the basic map operations.
	 */
	@Test
	public void testPutGetRemoveContainsTransform() throws Exception {
		RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.UNKNOWN,
				"test",
				IntSerializer.INSTANCE,
				new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		final MockInternalKeyContext<Integer> keyContext = new MockInternalKeyContext<>(IntSerializer.INSTANCE);

		final CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable =
			new CopyOnWriteStateTable<>(keyContext, metaInfo);

		ArrayList<Integer> state_1_1 = new ArrayList<>();
		state_1_1.add(41);
		ArrayList<Integer> state_2_1 = new ArrayList<>();
		state_2_1.add(42);
		ArrayList<Integer> state_1_2 = new ArrayList<>();
		state_1_2.add(43);

		Assert.assertNull(stateTable.putAndGetOld(1, 1, state_1_1));
		Assert.assertEquals(state_1_1, stateTable.get(1, 1));
		Assert.assertEquals(1, stateTable.size());

		Assert.assertNull(stateTable.putAndGetOld(2, 1, state_2_1));
		Assert.assertEquals(state_2_1, stateTable.get(2, 1));
		Assert.assertEquals(2, stateTable.size());

		Assert.assertNull(stateTable.putAndGetOld(1, 2, state_1_2));
		Assert.assertEquals(state_1_2, stateTable.get(1, 2));
		Assert.assertEquals(3, stateTable.size());

		Assert.assertTrue(stateTable.containsKey(2, 1));
		Assert.assertFalse(stateTable.containsKey(3, 1));
		Assert.assertFalse(stateTable.containsKey(2, 3));
		stateTable.put(2, 1, null);
		Assert.assertTrue(stateTable.containsKey(2, 1));
		Assert.assertEquals(3, stateTable.size());
		Assert.assertNull(stateTable.get(2, 1));
		stateTable.put(2, 1, state_2_1);
		Assert.assertEquals(3, stateTable.size());

		Assert.assertEquals(state_2_1, stateTable.removeAndGetOld(2, 1));
		Assert.assertFalse(stateTable.containsKey(2, 1));
		Assert.assertEquals(2, stateTable.size());

		stateTable.remove(1, 2);
		Assert.assertFalse(stateTable.containsKey(1, 2));
		Assert.assertEquals(1, stateTable.size());

		Assert.assertNull(stateTable.removeAndGetOld(4, 2));
		Assert.assertEquals(1, stateTable.size());

		StateTransformationFunction<ArrayList<Integer>, Integer> function =
			new StateTransformationFunction<ArrayList<Integer>, Integer>() {
				@Override
				public ArrayList<Integer> apply(ArrayList<Integer> previousState, Integer value) throws Exception {
					previousState.add(value);
					return previousState;
				}
			};

		final int value = 4711;
		stateTable.transform(1, 1, value, function);
		state_1_1 = function.apply(state_1_1, value);
		Assert.assertEquals(state_1_1, stateTable.get(1, 1));
	}

	/**
	 * This test triggers incremental rehash and tests for corruptions.
	 */
	@Test
	public void testIncrementalRehash() {
		RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.UNKNOWN,
				"test",
				IntSerializer.INSTANCE,
				new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		final MockInternalKeyContext<Integer> keyContext = new MockInternalKeyContext<>(IntSerializer.INSTANCE);

		final CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable =
			new CopyOnWriteStateTable<>(keyContext, metaInfo);

		int insert = 0;
		int remove = 0;
		while (!stateTable.isRehashing()) {
			stateTable.put(insert++, 0, new ArrayList<Integer>());
			if (insert % 8 == 0) {
				stateTable.remove(remove++, 0);
			}
		}
		Assert.assertEquals(insert - remove, stateTable.size());
		while (stateTable.isRehashing()) {
			stateTable.put(insert++, 0, new ArrayList<Integer>());
			if (insert % 8 == 0) {
				stateTable.remove(remove++, 0);
			}
		}
		Assert.assertEquals(insert - remove, stateTable.size());

		for (int i = 0; i < insert; ++i) {
			if (i < remove) {
				Assert.assertFalse(stateTable.containsKey(i, 0));
			} else {
				Assert.assertTrue(stateTable.containsKey(i, 0));
			}
		}
	}

	/**
	 * This test does some random modifications to a state table and a reference (hash map). Then draws snapshots,
	 * performs more modifications and checks snapshot integrity.
	 */
	@Test
	public void testRandomModificationsAndCopyOnWriteIsolation() throws Exception {

		final RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.UNKNOWN,
				"test",
				IntSerializer.INSTANCE,
				new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		final MockInternalKeyContext<Integer> keyContext = new MockInternalKeyContext<>(IntSerializer.INSTANCE);

		final CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable =
			new CopyOnWriteStateTable<>(keyContext, metaInfo);

		final HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> referenceMap = new HashMap<>();

		final Random random = new Random(42);

		// holds snapshots from the map under test
		CopyOnWriteStateTable.StateTableEntry<Integer, Integer, ArrayList<Integer>>[] snapshot = null;
		int snapshotSize = 0;

		// holds a reference snapshot from our reference map that we compare against
		Tuple3<Integer, Integer, ArrayList<Integer>>[] reference = null;

		int val = 0;


		int snapshotCounter = 0;
		int referencedSnapshotId = 0;

		final StateTransformationFunction<ArrayList<Integer>, Integer> transformationFunction =
			new StateTransformationFunction<ArrayList<Integer>, Integer>() {
				@Override
				public ArrayList<Integer> apply(ArrayList<Integer> previousState, Integer value) throws Exception {
					if (previousState == null) {
						previousState = new ArrayList<>();
					}
					previousState.add(value);
					// we give back the original, attempting to spot errors in to copy-on-write
					return previousState;
				}
			};

		// the main loop for modifications
		for (int i = 0; i < 10_000_000; ++i) {

			int key = random.nextInt(20);
			int namespace = random.nextInt(4);
			Tuple2<Integer, Integer> compositeKey = new Tuple2<>(key, namespace);

			int op = random.nextInt(7);

			ArrayList<Integer> state = null;
			ArrayList<Integer> referenceState = null;

			switch (op) {
				case 0:
				case 1: {
					state = stateTable.get(key, namespace);
					referenceState = referenceMap.get(compositeKey);
					if (null == state) {
						state = new ArrayList<>();
						stateTable.put(key, namespace, state);
						referenceState = new ArrayList<>();
						referenceMap.put(compositeKey, referenceState);
					}
					break;
				}
				case 2: {
					stateTable.put(key, namespace, new ArrayList<Integer>());
					referenceMap.put(compositeKey, new ArrayList<Integer>());
					break;
				}
				case 3: {
					state = stateTable.putAndGetOld(key, namespace, new ArrayList<Integer>());
					referenceState = referenceMap.put(compositeKey, new ArrayList<Integer>());
					break;
				}
				case 4: {
					stateTable.remove(key, namespace);
					referenceMap.remove(compositeKey);
					break;
				}
				case 5: {
					state = stateTable.removeAndGetOld(key, namespace);
					referenceState = referenceMap.remove(compositeKey);
					break;
				}
				case 6: {
					final int updateValue = random.nextInt(1000);
					stateTable.transform(key, namespace, updateValue, transformationFunction);
					referenceMap.put(compositeKey, transformationFunction.apply(
						referenceMap.remove(compositeKey), updateValue));
					break;
				}
				default: {
					Assert.fail("Unknown op-code " + op);
				}
			}

			Assert.assertEquals(referenceMap.size(), stateTable.size());

			if (state != null) {
				// mutate the states a bit...
				if (random.nextBoolean() && !state.isEmpty()) {
					state.remove(state.size() - 1);
					referenceState.remove(referenceState.size() - 1);
				} else {
					state.add(val);
					referenceState.add(val);
					++val;
				}
			}

			Assert.assertEquals(referenceState, state);

			// snapshot triggering / comparison / release
			if (i > 0 && i % 500 == 0) {

				if (snapshot != null) {
					// check our referenced snapshot
					deepCheck(reference, convert(snapshot, snapshotSize));

					if (i % 1_000 == 0) {
						// draw and release some other snapshot while holding on the old snapshot
						++snapshotCounter;
						stateTable.snapshotTableArrays();
						stateTable.releaseSnapshot(snapshotCounter);
					}

					//release the snapshot after some time
					if (i % 5_000 == 0) {
						snapshot = null;
						reference = null;
						snapshotSize = 0;
						stateTable.releaseSnapshot(referencedSnapshotId);
					}

				} else {
					// if there is no more referenced snapshot, we create one
					++snapshotCounter;
					referencedSnapshotId = snapshotCounter;
					snapshot = stateTable.snapshotTableArrays();
					snapshotSize = stateTable.size();
					reference = manualDeepDump(referenceMap);
				}
			}
		}
	}

	/**
	 * This tests for the copy-on-write contracts, e.g. ensures that no copy-on-write is active after all snapshots are
	 * released.
	 */
	@Test
	public void testCopyOnWriteContracts() {
		RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
			new RegisteredKeyValueStateBackendMetaInfo<>(
				StateDescriptor.Type.UNKNOWN,
				"test",
				IntSerializer.INSTANCE,
				new ArrayListSerializer<>(IntSerializer.INSTANCE)); // we use mutable state objects.

		final MockInternalKeyContext<Integer> keyContext = new MockInternalKeyContext<>(IntSerializer.INSTANCE);

		final CopyOnWriteStateTable<Integer, Integer, ArrayList<Integer>> stateTable =
			new CopyOnWriteStateTable<>(keyContext, metaInfo);

		ArrayList<Integer> originalState1 = new ArrayList<>(1);
		ArrayList<Integer> originalState2 = new ArrayList<>(1);
		ArrayList<Integer> originalState3 = new ArrayList<>(1);
		ArrayList<Integer> originalState4 = new ArrayList<>(1);
		ArrayList<Integer> originalState5 = new ArrayList<>(1);

		originalState1.add(1);
		originalState2.add(2);
		originalState3.add(3);
		originalState4.add(4);
		originalState5.add(5);

		stateTable.put(1, 1, originalState1);
		stateTable.put(2, 1, originalState2);
		stateTable.put(4, 1, originalState4);
		stateTable.put(5, 1, originalState5);

		// no snapshot taken, we get the original back
		Assert.assertTrue(stateTable.get(1, 1) == originalState1);
		CopyOnWriteStateTableSnapshot<Integer, Integer, ArrayList<Integer>> snapshot1 = stateTable.stateSnapshot();
		// after snapshot1 is taken, we get a copy...
		final ArrayList<Integer> copyState = stateTable.get(1, 1);
		Assert.assertFalse(copyState == originalState1);
		// ...and the copy is equal
		Assert.assertEquals(originalState1, copyState);

		// we make an insert AFTER snapshot1
		stateTable.put(3, 1, originalState3);

		// on repeated lookups, we get the same copy because no further snapshot was taken
		Assert.assertTrue(copyState == stateTable.get(1, 1));

		// we take snapshot2
		CopyOnWriteStateTableSnapshot<Integer, Integer, ArrayList<Integer>> snapshot2 = stateTable.stateSnapshot();
		// after the second snapshot, copy-on-write is active again for old entries
		Assert.assertFalse(copyState == stateTable.get(1, 1));
		// and equality still holds
		Assert.assertEquals(copyState, stateTable.get(1, 1));

		// after releasing snapshot2
		stateTable.releaseSnapshot(snapshot2);
		// we still get the original of the untouched late insert (after snapshot1)
		Assert.assertTrue(originalState3 == stateTable.get(3, 1));
		// but copy-on-write is still active for older inserts (before snapshot1)
		Assert.assertFalse(originalState4 == stateTable.get(4, 1));

		// after releasing snapshot1
		stateTable.releaseSnapshot(snapshot1);
		// no copy-on-write is active
		Assert.assertTrue(originalState5 == stateTable.get(5, 1));
	}

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

		final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
		InternalKeyContext<Integer> mockKeyContext = new InternalKeyContext<Integer>() {
			@Override
			public Integer getCurrentKey() {
				return 0;
			}

			@Override
			public int getCurrentKeyGroupIndex() {
				return 0;
			}

			@Override
			public int getNumberOfKeyGroups() {
				return 1;
			}

			@Override
			public KeyGroupRange getKeyGroupRange() {
				return keyGroupRange;
			}

			@Override
			public TypeSerializer<Integer> getKeySerializer() {
				return keySerializer;
			}
		};

		CopyOnWriteStateTable<Integer, Integer, Integer> table =
			new CopyOnWriteStateTable<>(mockKeyContext, metaInfo);

		table.put(0, 0, 0, 0);
		table.put(1, 0, 0, 1);
		table.put(2, 0, 1, 2);


		final CopyOnWriteStateTableSnapshot<Integer, Integer, Integer> snapshot = table.stateSnapshot();

		try {
			final StateSnapshot.StateKeyGroupWriter partitionedSnapshot = snapshot.getKeyGroupWriter();
			namespaceSerializer.disable();
			keySerializer.disable();
			stateSerializer.disable();

			partitionedSnapshot.writeStateInKeyGroup(
				new DataOutputViewStreamWrapper(
					new ByteArrayOutputStreamWithPos(1024)), 0);

		} finally {
			table.releaseSnapshot(snapshot);
		}
	}

	@SuppressWarnings("unchecked")
	private static <K, N, S> Tuple3<K, N, S>[] convert(CopyOnWriteStateTable.StateTableEntry<K, N, S>[] snapshot, int mapSize) {

		Tuple3<K, N, S>[] result = new Tuple3[mapSize];
		int pos = 0;
		for (CopyOnWriteStateTable.StateTableEntry<K, N, S> entry : snapshot) {
			while (null != entry) {
				result[pos++] = new Tuple3<>(entry.getKey(), entry.getNamespace(), entry.getState());
				entry = entry.next;
			}
		}
		Assert.assertEquals(mapSize, pos);
		return result;
	}

	@SuppressWarnings("unchecked")
	private Tuple3<Integer, Integer, ArrayList<Integer>>[] manualDeepDump(
		HashMap<Tuple2<Integer, Integer>,
			ArrayList<Integer>> map) {

		Tuple3<Integer, Integer, ArrayList<Integer>>[] result = new Tuple3[map.size()];
		int pos = 0;
		for (Map.Entry<Tuple2<Integer, Integer>, ArrayList<Integer>> entry : map.entrySet()) {
			Integer key = entry.getKey().f0;
			Integer namespace = entry.getKey().f1;
			result[pos++] = new Tuple3<>(key, namespace, new ArrayList<>(entry.getValue()));
		}
		return result;
	}

	private void deepCheck(
		Tuple3<Integer, Integer, ArrayList<Integer>>[] a,
		Tuple3<Integer, Integer, ArrayList<Integer>>[] b) {

		if (a == b) {
			return;
		}

		Assert.assertEquals(a.length, b.length);

		Comparator<Tuple3<Integer, Integer, ArrayList<Integer>>> comparator =
			new Comparator<Tuple3<Integer, Integer, ArrayList<Integer>>>() {

				@Override
				public int compare(Tuple3<Integer, Integer, ArrayList<Integer>> o1, Tuple3<Integer, Integer, ArrayList<Integer>> o2) {
					int namespaceDiff = o1.f1 - o2.f1;
					return namespaceDiff != 0 ? namespaceDiff : o1.f0 - o2.f0;
				}
			};

		Arrays.sort(a, comparator);
		Arrays.sort(b, comparator);

		for (int i = 0; i < a.length; ++i) {
			Tuple3<Integer, Integer, ArrayList<Integer>> av = a[i];
			Tuple3<Integer, Integer, ArrayList<Integer>> bv = b[i];

			Assert.assertEquals(av.f0, bv.f0);
			Assert.assertEquals(av.f1, bv.f1);
			Assert.assertEquals(av.f2, bv.f2);
		}
	}

	static class MockInternalKeyContext<T> implements InternalKeyContext<T> {

		private T key;
		private final TypeSerializer<T> serializer;
		private final KeyGroupRange keyGroupRange;

		public MockInternalKeyContext(TypeSerializer<T> serializer) {
			this.serializer = serializer;
			this.keyGroupRange = new KeyGroupRange(0, 0);
		}

		public void setKey(T key) {
			this.key = key;
		}

		@Override
		public T getCurrentKey() {
			return key;
		}

		@Override
		public int getCurrentKeyGroupIndex() {
			return 0;
		}

		@Override
		public int getNumberOfKeyGroups() {
			return 1;
		}

		@Override
		public KeyGroupRange getKeyGroupRange() {
			return keyGroupRange;
		}

		@Override
		public TypeSerializer<T> getKeySerializer() {
			return serializer;
		}
	}

}
