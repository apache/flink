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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A {@code SimpleVersionedSerializer} used to serialize the {@link BucketState BucketState}.
 */
@Internal
class BucketStateSerializer<BucketID> implements SimpleVersionedSerializer<BucketState<BucketID>> {

	private static final int MAGIC_NUMBER = 0x1e764b79;

	private final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer;

	private final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitableSerializer;

	private final SimpleVersionedSerializer<BucketID> bucketIdSerializer;

	BucketStateSerializer(
			final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer,
			final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitableSerializer,
			final SimpleVersionedSerializer<BucketID> bucketIdSerializer
	) {
		this.resumableSerializer = Preconditions.checkNotNull(resumableSerializer);
		this.commitableSerializer = Preconditions.checkNotNull(commitableSerializer);
		this.bucketIdSerializer = Preconditions.checkNotNull(bucketIdSerializer);
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public byte[] serialize(BucketState<BucketID> state) throws IOException {
		DataOutputSerializer out = new DataOutputSerializer(256);
		out.writeInt(MAGIC_NUMBER);
		serializeV1(state, out);
		return out.getCopyOfBuffer();
	}

	@Override
	public BucketState<BucketID> deserialize(int version, byte[] serialized) throws IOException {
		switch (version) {
			case 1:
				DataInputDeserializer in = new DataInputDeserializer(serialized);
				validateMagicNumber(in);
				return deserializeV1(in);
			default:
				throw new IOException("Unrecognized version or corrupt state: " + version);
		}
	}

	@VisibleForTesting
	void serializeV1(BucketState<BucketID> state, DataOutputView out) throws IOException {
		SimpleVersionedSerialization.writeVersionAndSerialize(bucketIdSerializer, state.getBucketId(), out);
		out.writeUTF(state.getBucketPath().toString());
		out.writeLong(state.getInProgressFileCreationTime());

		// put the current open part file
		if (state.hasInProgressResumableFile()) {
			final RecoverableWriter.ResumeRecoverable resumable = state.getInProgressResumableFile();
			out.writeBoolean(true);
			SimpleVersionedSerialization.writeVersionAndSerialize(resumableSerializer, resumable, out);
		}
		else {
			out.writeBoolean(false);
		}

		// put the map of pending files per checkpoint
		final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingCommitters = state.getCommittableFilesPerCheckpoint();

		// manually keep the version here to safe some bytes
		out.writeInt(commitableSerializer.getVersion());

		out.writeInt(pendingCommitters.size());
		for (Entry<Long, List<RecoverableWriter.CommitRecoverable>> resumablesForCheckpoint : pendingCommitters.entrySet()) {
			List<RecoverableWriter.CommitRecoverable> resumables = resumablesForCheckpoint.getValue();

			out.writeLong(resumablesForCheckpoint.getKey());
			out.writeInt(resumables.size());

			for (RecoverableWriter.CommitRecoverable resumable : resumables) {
				byte[] serialized = commitableSerializer.serialize(resumable);
				out.writeInt(serialized.length);
				out.write(serialized);
			}
		}
	}

	@VisibleForTesting
	BucketState<BucketID> deserializeV1(DataInputView in) throws IOException {
		final BucketID bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(bucketIdSerializer, in);
		final String bucketPathStr = in.readUTF();
		final long creationTime = in.readLong();

		// then get the current resumable stream
		RecoverableWriter.ResumeRecoverable current = null;
		if (in.readBoolean()) {
			current = SimpleVersionedSerialization.readVersionAndDeSerialize(resumableSerializer, in);
		}

		final int committableVersion = in.readInt();
		final int numCheckpoints = in.readInt();
		final HashMap<Long, List<RecoverableWriter.CommitRecoverable>> resumablesPerCheckpoint = new HashMap<>(numCheckpoints);

		for (int i = 0; i < numCheckpoints; i++) {
			final long checkpointId = in.readLong();
			final int noOfResumables = in.readInt();

			final List<RecoverableWriter.CommitRecoverable> resumables = new ArrayList<>(noOfResumables);
			for (int j = 0; j < noOfResumables; j++) {
				final byte[] bytes = new byte[in.readInt()];
				in.readFully(bytes);
				resumables.add(commitableSerializer.deserialize(committableVersion, bytes));
			}
			resumablesPerCheckpoint.put(checkpointId, resumables);
		}

		return new BucketState<>(
				bucketId,
				new Path(bucketPathStr),
				creationTime,
				current,
				resumablesPerCheckpoint);
	}

	private static void validateMagicNumber(DataInputView in) throws IOException {
		final int magicNumber = in.readInt();
		if (magicNumber != MAGIC_NUMBER) {
			throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
		}
	}
}
