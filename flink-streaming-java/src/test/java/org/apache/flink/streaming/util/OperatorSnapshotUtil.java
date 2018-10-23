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

package org.apache.flink.streaming.util;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2Serializer;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Util for writing/reading {@link OperatorSubtaskState},
 * for use in tests.
 */
public class OperatorSnapshotUtil {

	public static String getResourceFilename(String filename) {
		ClassLoader cl = OperatorSnapshotUtil.class.getClassLoader();
		URL resource = cl.getResource(filename);
		return resource.getFile();
	}

	public static void writeStateHandle(OperatorSubtaskState state, String path) throws IOException {
		FileOutputStream out = new FileOutputStream(path);

		try (DataOutputStream dos = new DataOutputStream(out)) {

			// required for backwards compatibility.
			dos.writeInt(0);

			// still required for compatibility
			SavepointV2Serializer.serializeStreamStateHandle(null, dos);

			Collection<OperatorStateHandle> rawOperatorState = state.getRawOperatorState();
			if (rawOperatorState != null) {
				dos.writeInt(rawOperatorState.size());
				for (OperatorStateHandle operatorStateHandle : rawOperatorState) {
					SavepointV2Serializer.serializeOperatorStateHandle(operatorStateHandle, dos);
				}
			} else {
				// this means no states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<OperatorStateHandle> managedOperatorState = state.getManagedOperatorState();
			if (managedOperatorState != null) {
				dos.writeInt(managedOperatorState.size());
				for (OperatorStateHandle operatorStateHandle : managedOperatorState) {
					SavepointV2Serializer.serializeOperatorStateHandle(operatorStateHandle, dos);
				}
			} else {
				// this means no states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<KeyedStateHandle> rawKeyedState = state.getRawKeyedState();
			if (rawKeyedState != null) {
				dos.writeInt(rawKeyedState.size());
				for (KeyedStateHandle keyedStateHandle : rawKeyedState) {
					SavepointV2Serializer.serializeKeyedStateHandle(keyedStateHandle, dos);
				}
			} else {
				// this means no operator states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<KeyedStateHandle> managedKeyedState = state.getManagedKeyedState();
			if (managedKeyedState != null) {
				dos.writeInt(managedKeyedState.size());
				for (KeyedStateHandle keyedStateHandle : managedKeyedState) {
					SavepointV2Serializer.serializeKeyedStateHandle(keyedStateHandle, dos);
				}
			} else {
				// this means no operator states, not even an empty list
				dos.writeInt(-1);
			}

			dos.flush();
		}
	}

	public static OperatorSubtaskState readStateHandle(String path) throws IOException, ClassNotFoundException {
		FileInputStream in = new FileInputStream(path);
		try (DataInputStream dis = new DataInputStream(in)) {

			// required for backwards compatibility.
			dis.readInt();

			// still required for compatibility to consume the bytes.
			SavepointV2Serializer.deserializeStreamStateHandle(dis);

			List<OperatorStateHandle> rawOperatorState = null;
			int numRawOperatorStates = dis.readInt();
			if (numRawOperatorStates >= 0) {
				rawOperatorState = new ArrayList<>();
				for (int i = 0; i < numRawOperatorStates; i++) {
					OperatorStateHandle operatorState = SavepointV2Serializer.deserializeOperatorStateHandle(
						dis);
					rawOperatorState.add(operatorState);
				}
			}

			List<OperatorStateHandle> managedOperatorState = null;
			int numManagedOperatorStates = dis.readInt();
			if (numManagedOperatorStates >= 0) {
				managedOperatorState = new ArrayList<>();
				for (int i = 0; i < numManagedOperatorStates; i++) {
					OperatorStateHandle operatorState = SavepointV2Serializer.deserializeOperatorStateHandle(
						dis);
					managedOperatorState.add(operatorState);
				}
			}

			List<KeyedStateHandle> rawKeyedState = null;
			int numRawKeyedStates = dis.readInt();
			if (numRawKeyedStates >= 0) {
				rawKeyedState = new ArrayList<>();
				for (int i = 0; i < numRawKeyedStates; i++) {
					KeyedStateHandle keyedState = SavepointV2Serializer.deserializeKeyedStateHandle(
						dis);
					rawKeyedState.add(keyedState);
				}
			}

			List<KeyedStateHandle> managedKeyedState = null;
			int numManagedKeyedStates = dis.readInt();
			if (numManagedKeyedStates >= 0) {
				managedKeyedState = new ArrayList<>();
				for (int i = 0; i < numManagedKeyedStates; i++) {
					KeyedStateHandle keyedState = SavepointV2Serializer.deserializeKeyedStateHandle(
						dis);
					managedKeyedState.add(keyedState);
				}
			}

			return new OperatorSubtaskState(
				new StateObjectCollection<>(managedOperatorState),
				new StateObjectCollection<>(rawOperatorState),
				new StateObjectCollection<>(managedKeyedState),
				new StateObjectCollection<>(rawKeyedState));
		}
	}
}
