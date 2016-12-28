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

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.runtime.checkpoint.savepoint.SavepointV0;
import org.apache.flink.migration.runtime.checkpoint.savepoint.SavepointV0Serializer;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.memory.MemValueState;
import org.apache.flink.migration.runtime.state.memory.SerializedStateHandle;
import org.apache.flink.migration.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.migration.streaming.runtime.tasks.StreamTaskStateList;
import org.apache.flink.migration.util.MigrationInstantiationUtil;
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

		Collection<org.apache.flink.migration.runtime.checkpoint.TaskState> expected =
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
						HashMap<String, KvStateSnapshot<?, ?, ?, ?>> testKeyedState =
								MigrationInstantiationUtil.deserializeObject(stateHandle.getData(), cl);

						assertEquals(2, testKeyedState.size());
						for (KvStateSnapshot<?, ?, ?, ?> snapshot : testKeyedState.values()) {
							MemValueState.Snapshot<?, ?, ?> castedSnapshot = (MemValueState.Snapshot<?, ?, ?>) snapshot;
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
			SavepointStore.removeSavepoint(path.toString());
		}
	}

	private static Collection<org.apache.flink.migration.runtime.checkpoint.TaskState> createTaskStatesOld(
			int numTaskStates, int numSubtaskStates) throws Exception {

		List<org.apache.flink.migration.runtime.checkpoint.TaskState> taskStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			org.apache.flink.migration.runtime.checkpoint.TaskState taskState =
					new org.apache.flink.migration.runtime.checkpoint.TaskState(new JobVertexID(), numSubtaskStates);
			for (int j = 0; j < numSubtaskStates; j++) {

				StreamTaskState[] streamTaskStates = new StreamTaskState[2];

				for (int k = 0; k < streamTaskStates.length; k++) {
					StreamTaskState state = new StreamTaskState();
					Tuple4<Integer, Integer, Integer, Integer> testState = new Tuple4<>(0, i, j, k);
					if (j % 4 != 0) {
						state.setFunctionState(new SerializedStateHandle<Serializable>(testState));
					}
					testState = new Tuple4<>(1, i, j, k);
					state.setOperatorState(new SerializedStateHandle<>(testState));

					if ((0 == k) && (i % 3 != 0)) {
						HashMap<String, KvStateSnapshot<?, ?, ?, ?>> testKeyedState = new HashMap<>(2);
						for (int l = 0; l < 2; ++l) {
							String name = "keyed-" + l;
							KvStateSnapshot<?, ?, ?, ?> testKeyedSnapshot =
									new MemValueState.Snapshot<>(
											IntSerializer.INSTANCE,
											VoidNamespaceSerializer.INSTANCE,
											IntSerializer.INSTANCE,
											new ValueStateDescriptor<>(name, Integer.class, 0),
											new byte[]{(byte) i, (byte) j});
							testKeyedState.put(name, testKeyedSnapshot);
						}
						state.setKvStates(testKeyedState);
					}
					streamTaskStates[k] = state;
				}

				StreamTaskStateList streamTaskStateList = new StreamTaskStateList(streamTaskStates);
				org.apache.flink.migration.util.SerializedValue<
						org.apache.flink.migration.runtime.state.StateHandle<?>> handle =
						new org.apache.flink.migration.util.SerializedValue<
								org.apache.flink.migration.runtime.state.StateHandle<?>>(streamTaskStateList);

				taskState.putState(j, new org.apache.flink.migration.runtime.checkpoint.SubtaskState(handle, 0, 0));
			}

			taskStates.add(taskState);
		}

		return taskStates;
	}
}
