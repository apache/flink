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

package org.apache.flink.runtime.state;

import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialization proxy for all meta data in operator state backends. In the future we might also requiresMigration the actual state
 * serialization logic here.
 */
public class OperatorBackendSerializationProxy extends VersionedIOReadableWritable {

	public static final int VERSION = 3;

	private List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> operatorStateMetaInfoSnapshots;
	private List<RegisteredBroadcastBackendStateMetaInfo.Snapshot<?, ?>> broadcastStateMetaInfoSnapshots;
	private ClassLoader userCodeClassLoader;

	public OperatorBackendSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	public OperatorBackendSerializationProxy(
			List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> operatorStateMetaInfoSnapshots,
			List<RegisteredBroadcastBackendStateMetaInfo.Snapshot<?, ?>> broadcastStateMetaInfoSnapshots) {

		this.operatorStateMetaInfoSnapshots = Preconditions.checkNotNull(operatorStateMetaInfoSnapshots);
		this.broadcastStateMetaInfoSnapshots = Preconditions.checkNotNull(broadcastStateMetaInfoSnapshots);
		Preconditions.checkArgument(
				operatorStateMetaInfoSnapshots.size() <= Short.MAX_VALUE &&
						broadcastStateMetaInfoSnapshots.size() <= Short.MAX_VALUE
		);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public int[] getCompatibleVersions() {
		// we are compatible with version 3 (Flink 1.5.x), 2 (Flink 1.4.x, Flink 1.3.x) and version 1 (Flink 1.2.x)
		return new int[] {VERSION, 2, 1};
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		out.writeShort(operatorStateMetaInfoSnapshots.size());
		for (RegisteredOperatorBackendStateMetaInfo.Snapshot<?> state : operatorStateMetaInfoSnapshots) {
			OperatorBackendStateMetaInfoSnapshotReaderWriters
					.getOperatorStateWriterForVersion(VERSION, state)
					.writeOperatorStateMetaInfo(out);
		}

		out.writeShort(broadcastStateMetaInfoSnapshots.size());
		for (RegisteredBroadcastBackendStateMetaInfo.Snapshot<?, ?> state : broadcastStateMetaInfoSnapshots) {
			OperatorBackendStateMetaInfoSnapshotReaderWriters
					.getBroadcastStateWriterForVersion(VERSION, state)
					.writeBroadcastStateMetaInfo(out);
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		int numOperatorStates = in.readShort();
		operatorStateMetaInfoSnapshots = new ArrayList<>(numOperatorStates);
		for (int i = 0; i < numOperatorStates; i++) {
			operatorStateMetaInfoSnapshots.add(
					OperatorBackendStateMetaInfoSnapshotReaderWriters
							.getOperatorStateReaderForVersion(getReadVersion(), userCodeClassLoader)
							.readOperatorStateMetaInfo(in));
		}

		if (getReadVersion() >= 3) {
			// broadcast states did not exist prior to version 3
			int numBroadcastStates = in.readShort();
			broadcastStateMetaInfoSnapshots = new ArrayList<>(numBroadcastStates);
			for (int i = 0; i < numBroadcastStates; i++) {
				broadcastStateMetaInfoSnapshots.add(
						OperatorBackendStateMetaInfoSnapshotReaderWriters
								.getBroadcastStateReaderForVersion(getReadVersion(), userCodeClassLoader)
								.readBroadcastStateMetaInfo(in));
			}
		} else {
			broadcastStateMetaInfoSnapshots = new ArrayList<>();
		}
	}

	public List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> getOperatorStateMetaInfoSnapshots() {
		return operatorStateMetaInfoSnapshots;
	}

	public List<RegisteredBroadcastBackendStateMetaInfo.Snapshot<?, ?>> getBroadcastStateMetaInfoSnapshots() {
		return broadcastStateMetaInfoSnapshots;
	}
}
