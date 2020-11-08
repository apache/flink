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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.PendingSplitsCheckpointSerializer;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * SerDe for {@link ContinuousHivePendingSplitsCheckpoint}.
 */
public class ContinuousHivePendingSplitsCheckpointSerializer implements SimpleVersionedSerializer<PendingSplitsCheckpoint<HiveSourceSplit>> {

	private static final int VERSION = 1;

	private final PendingSplitsCheckpointSerializer<HiveSourceSplit> superSerDe;
	// SerDe for a single partition
	private final ListSerializer<String> partitionSerDe = new ListSerializer<>(StringSerializer.INSTANCE);
	// SerDe for the current read offset
	private final ReadOffsetSerDe readOffsetSerDe = ReadOffsetSerDeImpl.INSTANCE;

	public ContinuousHivePendingSplitsCheckpointSerializer(SimpleVersionedSerializer<HiveSourceSplit> splitSerDe) {
		superSerDe = new PendingSplitsCheckpointSerializer<>(splitSerDe);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public byte[] serialize(PendingSplitsCheckpoint<HiveSourceSplit> checkpoint) throws IOException {
		Preconditions.checkArgument(checkpoint.getClass() == ContinuousHivePendingSplitsCheckpoint.class,
				"Only supports %s", ContinuousHivePendingSplitsCheckpoint.class.getName());

		ContinuousHivePendingSplitsCheckpoint hiveCheckpoint = (ContinuousHivePendingSplitsCheckpoint) checkpoint;
		PendingSplitsCheckpoint<HiveSourceSplit> superCP = PendingSplitsCheckpoint.fromCollectionSnapshot(
				hiveCheckpoint.getSplits(), hiveCheckpoint.getAlreadyProcessedPaths());
		byte[] superBytes = superSerDe.serialize(superCP);
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

		try (DataOutputViewStreamWrapper outputWrapper = new DataOutputViewStreamWrapper(byteArrayOutputStream)) {
			outputWrapper.writeInt(superBytes.length);
			outputWrapper.write(superBytes);
			readOffsetSerDe.serialize(hiveCheckpoint.getCurrentReadOffset(), outputWrapper);
			outputWrapper.writeInt(hiveCheckpoint.getSeenPartitionsSinceOffset().size());
			for (List<String> partition : hiveCheckpoint.getSeenPartitionsSinceOffset()) {
				partitionSerDe.serialize(partition, outputWrapper);
			}
		}
		return byteArrayOutputStream.toByteArray();
	}

	@Override
	public PendingSplitsCheckpoint<HiveSourceSplit> deserialize(int version, byte[] serialized) throws IOException {
		if (version == 1) {
			try (DataInputViewStreamWrapper inputWrapper = new DataInputViewStreamWrapper(new ByteArrayInputStream(serialized))) {
				return deserializeV1(inputWrapper);
			}
		}
		throw new IOException("Unknown version: " + version);
	}

	private PendingSplitsCheckpoint<HiveSourceSplit> deserializeV1(DataInputViewStreamWrapper inputWrapper) throws IOException {
		byte[] superBytes = new byte[inputWrapper.readInt()];
		inputWrapper.readFully(superBytes);
		PendingSplitsCheckpoint<HiveSourceSplit> superCP = superSerDe.deserialize(superSerDe.getVersion(), superBytes);
		try {
			Comparable<?> currentReadOffset = readOffsetSerDe.deserialize(inputWrapper);
			int numParts = inputWrapper.readInt();
			List<List<String>> parts = new ArrayList<>(numParts);
			for (int i = 0; i < numParts; i++) {
				parts.add(partitionSerDe.deserialize(inputWrapper));
			}
			return new ContinuousHivePendingSplitsCheckpoint(superCP.getSplits(), currentReadOffset, parts);
		} catch (ClassNotFoundException e) {
			throw new IOException("Failed to deserialize " + getClass().getName(), e);
		}
	}

	private static class ReadOffsetSerDeImpl implements ReadOffsetSerDe {

		private static final ReadOffsetSerDeImpl INSTANCE = new ReadOffsetSerDeImpl();

		private ReadOffsetSerDeImpl() {
		}

		@Override
		public void serialize(Comparable<?> offset, OutputStream outputStream) throws IOException {
			InstantiationUtil.serializeObject(outputStream, offset);
		}

		@Override
		public Comparable<?> deserialize(InputStream inputStream) throws IOException, ClassNotFoundException {
			return InstantiationUtil.deserializeObject(inputStream, Thread.currentThread().getContextClassLoader());
		}
	}

	/**
	 * SerDe for the current read offset.
	 */
	interface ReadOffsetSerDe {

		void serialize(Comparable<?> offset, OutputStream outputStream) throws IOException;

		Comparable<?> deserialize(InputStream inputStream) throws IOException, ClassNotFoundException;
	}
}
