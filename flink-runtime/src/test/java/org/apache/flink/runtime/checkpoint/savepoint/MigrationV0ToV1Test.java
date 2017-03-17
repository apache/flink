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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.MigrationInstantiationUtil;
import org.apache.flink.migration.MigrationSerializedValue;
import org.apache.flink.migration.v0.SavepointV0;
import org.apache.flink.migration.v0.SavepointV0Serializer;
import org.apache.flink.migration.v0.api.ValueStateDescriptorV0;
import org.apache.flink.migration.v0.runtime.KvStateSnapshotV0;
import org.apache.flink.migration.v0.runtime.StateHandleV0;
import org.apache.flink.migration.v0.runtime.StreamTaskStateListV0;
import org.apache.flink.migration.v0.runtime.StreamTaskStateV0;
import org.apache.flink.migration.v0.runtime.SubtaskStateV0;
import org.apache.flink.migration.v0.runtime.TaskStateV0;
import org.apache.flink.migration.v0.runtime.memory.MemValueStateV0;
import org.apache.flink.migration.v0.runtime.memory.SerializedStateHandleV0;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("deprecation")
public class MigrationV0ToV1Test {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	/**
	 * Simple test of savepoint methods.
	 */
	@Test
	public void testSavepointMigrationV0ToV1() throws Exception {

		String target = tmp.getRoot().getAbsolutePath();

		assertEquals(0, tmp.getRoot().listFiles().length);

		long checkpointId = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE);
		int numTaskStates = 4;
		int numSubtaskStates = 16;

		Collection<TaskStateV0> expected =
				createTaskStatesOld(numTaskStates, numSubtaskStates);

		SavepointV0 savepoint = new SavepointV0(checkpointId, expected);

		assertEquals(SavepointV0.VERSION, savepoint.getVersion());
		assertEquals(checkpointId, savepoint.getCheckpointId());
		assertEquals(expected, savepoint.getOldTaskStates());

		assertFalse(savepoint.getOldTaskStates().isEmpty());

		Exception latestException = null;
		Path path = null;
		FSDataOutputStream fdos = null;

		FileSystem fs = null;

		try {

			// Try to create a FS output stream
			for (int attempt = 0; attempt < 10; attempt++) {
				path = new Path(target, FileUtils.getRandomFilename("savepoint-"));

				if (fs == null) {
					fs = FileSystem.get(path.toUri());
				}

				try {
					fdos = fs.create(path, false);
					break;
				} catch (Exception e) {
					latestException = e;
				}
			}

			if (fdos == null) {
				throw new IOException("Failed to create file output stream at " + path, latestException);
			}

			try (DataOutputStream dos = new DataOutputStream(fdos)) {
				dos.writeInt(SavepointStore.MAGIC_NUMBER);
				dos.writeInt(savepoint.getVersion());
				SavepointV0Serializer.INSTANCE.serializeOld(savepoint, dos);
			}

			ClassLoader cl = Thread.currentThread().getContextClassLoader();

			Savepoint sp = SavepointStore.loadSavepoint(path.toString(), cl);
			int t = 0;
			for (TaskState taskState : sp.getTaskStates()) {
				for (int p = 0; p < taskState.getParallelism(); ++p) {
					SubtaskState subtaskState = taskState.getState(p);
					ChainedStateHandle<StreamStateHandle> legacyOperatorState = subtaskState.getLegacyOperatorState();
					for (int c = 0; c < legacyOperatorState.getLength(); ++c) {
						StreamStateHandle stateHandle = legacyOperatorState.get(c);
						try (InputStream is = stateHandle.openInputStream()) {
							Tuple4<Integer, Integer, Integer, Integer> expTestState = new Tuple4<>(0, t, p, c);
							Tuple4<Integer, Integer, Integer, Integer> actTestState;
							//check function state
							if (p % 4 != 0) {
								assertEquals(1, is.read());
								actTestState = InstantiationUtil.deserializeObject(is, cl);
								assertEquals(expTestState, actTestState);
							} else {
								assertEquals(0, is.read());
							}

							//check operator state
							expTestState.f0 = 1;
							actTestState = InstantiationUtil.deserializeObject(is, cl);
							assertEquals(expTestState, actTestState);
						}
					}

					//check keyed state
					KeyGroupsStateHandle keyGroupsStateHandle = subtaskState.getManagedKeyedState();
					if (t % 3 != 0) {
						assertEquals(1, keyGroupsStateHandle.getNumberOfKeyGroups());
						assertEquals(p, keyGroupsStateHandle.getGroupRangeOffsets().getKeyGroupRange().getStartKeyGroup());

						ByteStreamStateHandle stateHandle =
								(ByteStreamStateHandle) keyGroupsStateHandle.getDelegateStateHandle();
						HashMap<String, KvStateSnapshotV0> testKeyedState =
								MigrationInstantiationUtil.deserializeObject(SavepointV0.VERSION, stateHandle.getData(), cl);

						assertEquals(2, testKeyedState.size());
						for (KvStateSnapshotV0 snapshot : testKeyedState.values()) {
							MemValueStateV0.Snapshot<?, ?, ?> castedSnapshot = (MemValueStateV0.Snapshot<?, ?, ?>) snapshot;
							byte[] data = castedSnapshot.getData();
							assertEquals(t, data[0]);
							assertEquals(p, data[1]);
						}
					} else {
						assertEquals(null, keyGroupsStateHandle);
					}
				}

				++t;
			}

			savepoint.dispose();

		} finally {
			// Dispose
			SavepointStore.removeSavepointFile(path.toString());
		}
	}

	private static Collection<TaskStateV0> createTaskStatesOld(
			int numTaskStates, int numSubtaskStates) throws Exception {

		List<TaskStateV0> taskStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			TaskStateV0 taskState = new TaskStateV0(new JobVertexID(), numSubtaskStates);
			for (int j = 0; j < numSubtaskStates; j++) {

				StreamTaskStateV0[] streamTaskStates = new StreamTaskStateV0[2];

				for (int k = 0; k < streamTaskStates.length; k++) {
					StreamTaskStateV0 state = new StreamTaskStateV0();
					Tuple4<Integer, Integer, Integer, Integer> testState = new Tuple4<>(0, i, j, k);
					if (j % 4 != 0) {
						state.setFunctionState(new SerializedStateHandleV0<Serializable>(testState));
					}
					testState = new Tuple4<>(1, i, j, k);
					state.setOperatorState(new SerializedStateHandleV0<>(testState));

					if ((0 == k) && (i % 3 != 0)) {
						HashMap<String, KvStateSnapshotV0<?, ?, ?, ?>> testKeyedState = new HashMap<>(2);
						for (int l = 0; l < 2; ++l) {
							String name = "keyed-" + l;
							KvStateSnapshotV0 testKeyedSnapshot =
									new MemValueStateV0.Snapshot<>(
											IntSerializer.INSTANCE,
											VoidNamespaceSerializer.INSTANCE,
											IntSerializer.INSTANCE,
											new ValueStateDescriptorV0<>(name, IntSerializer.INSTANCE, 0),
											new byte[]{(byte) i, (byte) j});
							testKeyedState.put(name, testKeyedSnapshot);
						}
						state.setKvStates(testKeyedState);
					}
					streamTaskStates[k] = state;
				}

				StreamTaskStateListV0 streamTaskStateList = new StreamTaskStateListV0(streamTaskStates);
				MigrationSerializedValue<StateHandleV0<?>> handle = new MigrationSerializedValue<StateHandleV0<?>>(streamTaskStateList);

				taskState.putState(j, new SubtaskStateV0(handle, 0, 0));
			}

			taskStates.add(taskState);
		}

		return taskStates;
	}
}
