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
import org.apache.flink.runtime.checkpoint.metadata.MetadataV3Serializer;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;

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
			dos.writeInt(MetadataV3Serializer.VERSION);

			// still required for compatibility
			MetadataV3Serializer.serializeStreamStateHandle(null, dos);

			Collection<OperatorStateHandle> rawOperatorState = state.getRawOperatorState();
			if (rawOperatorState != null) {
				dos.writeInt(rawOperatorState.size());
				for (OperatorStateHandle operatorStateHandle : rawOperatorState) {
					MetadataV3Serializer.serializeOperatorStateHandleUtil(operatorStateHandle, dos);
				}
			} else {
				// this means no states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<OperatorStateHandle> managedOperatorState = state.getManagedOperatorState();
			if (managedOperatorState != null) {
				dos.writeInt(managedOperatorState.size());
				for (OperatorStateHandle operatorStateHandle : managedOperatorState) {
					MetadataV3Serializer.serializeOperatorStateHandleUtil(operatorStateHandle, dos);
				}
			} else {
				// this means no states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<KeyedStateHandle> rawKeyedState = state.getRawKeyedState();
			if (rawKeyedState != null) {
				dos.writeInt(rawKeyedState.size());
				for (KeyedStateHandle keyedStateHandle : rawKeyedState) {
					MetadataV3Serializer.serializeKeyedStateHandleUtil(keyedStateHandle, dos);
				}
			} else {
				// this means no operator states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<KeyedStateHandle> managedKeyedState = state.getManagedKeyedState();
			if (managedKeyedState != null) {
				dos.writeInt(managedKeyedState.size());
				for (KeyedStateHandle keyedStateHandle : managedKeyedState) {
					MetadataV3Serializer.serializeKeyedStateHandleUtil(keyedStateHandle, dos);
				}
			} else {
				// this means no operator states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<InputChannelStateHandle> inputChannelStateHandles  = state.getInputChannelState();
			dos.writeInt(inputChannelStateHandles.size());
			for (InputChannelStateHandle inputChannelStateHandle : inputChannelStateHandles) {
				MetadataV3Serializer.INSTANCE.serializeInputChannelStateHandle(inputChannelStateHandle, dos);
			}

			Collection<ResultSubpartitionStateHandle> resultSubpartitionStateHandles  = state.getResultSubpartitionState();
			dos.writeInt(inputChannelStateHandles.size());
			for (ResultSubpartitionStateHandle resultSubpartitionStateHandle : resultSubpartitionStateHandles) {
				MetadataV3Serializer.INSTANCE.serializeResultSubpartitionStateHandle(resultSubpartitionStateHandle, dos);
			}

			dos.flush();
		}
	}

	public static OperatorSubtaskState readStateHandle(String path) throws IOException, ClassNotFoundException {
		FileInputStream in = new FileInputStream(path);
		try (DataInputStream dis = new DataInputStream(in)) {

			// required for backwards compatibility.
			final int v = dis.readInt();

			// still required for compatibility to consume the bytes.
			MetadataV3Serializer.deserializeStreamStateHandle(dis);

			List<OperatorStateHandle> rawOperatorState = null;
			int numRawOperatorStates = dis.readInt();
			if (numRawOperatorStates >= 0) {
				rawOperatorState = new ArrayList<>();
				for (int i = 0; i < numRawOperatorStates; i++) {
					OperatorStateHandle operatorState = MetadataV3Serializer.deserializeOperatorStateHandleUtil(dis);
					rawOperatorState.add(operatorState);
				}
			}

			List<OperatorStateHandle> managedOperatorState = null;
			int numManagedOperatorStates = dis.readInt();
			if (numManagedOperatorStates >= 0) {
				managedOperatorState = new ArrayList<>();
				for (int i = 0; i < numManagedOperatorStates; i++) {
					OperatorStateHandle operatorState = MetadataV3Serializer.deserializeOperatorStateHandleUtil(dis);
					managedOperatorState.add(operatorState);
				}
			}

			List<KeyedStateHandle> rawKeyedState = null;
			int numRawKeyedStates = dis.readInt();
			if (numRawKeyedStates >= 0) {
				rawKeyedState = new ArrayList<>();
				for (int i = 0; i < numRawKeyedStates; i++) {
					KeyedStateHandle keyedState = MetadataV3Serializer.deserializeKeyedStateHandleUtil(dis);
					rawKeyedState.add(keyedState);
				}
			}

			List<KeyedStateHandle> managedKeyedState = null;
			int numManagedKeyedStates = dis.readInt();
			if (numManagedKeyedStates >= 0) {
				managedKeyedState = new ArrayList<>();
				for (int i = 0; i < numManagedKeyedStates; i++) {
					KeyedStateHandle keyedState = MetadataV3Serializer.deserializeKeyedStateHandleUtil(dis);
					managedKeyedState.add(keyedState);
				}
			}

			final StateObjectCollection<InputChannelStateHandle> inputChannelStateHandles =
				v == MetadataV3Serializer.VERSION ?
					MetadataV3Serializer.deserializeInputChannelStateHandle(dis) :
					StateObjectCollection.empty();

			final StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionStateHandles =
				v == MetadataV3Serializer.VERSION ?
					MetadataV3Serializer.INSTANCE.deserializeResultSubpartitionStateHandle(dis) :
					StateObjectCollection.empty();

			return new OperatorSubtaskState(
				new StateObjectCollection<>(managedOperatorState),
				new StateObjectCollection<>(rawOperatorState),
				new StateObjectCollection<>(managedKeyedState),
				new StateObjectCollection<>(rawKeyedState),
				inputChannelStateHandles,
				resultSubpartitionStateHandles);
		}
	}
}
