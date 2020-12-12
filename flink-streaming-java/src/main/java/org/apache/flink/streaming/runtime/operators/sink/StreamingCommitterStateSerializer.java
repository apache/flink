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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The serializer for the {@link StreamingCommitterState}.
 */

final class StreamingCommitterStateSerializer<CommT> implements SimpleVersionedSerializer<StreamingCommitterState<CommT>> {

	private static final int MAGIC_NUMBER = 0xb91f252c;

	private final SimpleVersionedSerializer<CommT> committableSerializer;

	StreamingCommitterStateSerializer(SimpleVersionedSerializer<CommT> committableSerializer) {
		this.committableSerializer = checkNotNull(committableSerializer);
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public byte[] serialize(StreamingCommitterState<CommT> state) throws IOException {
		DataOutputSerializer out = new DataOutputSerializer(256);
		out.writeInt(MAGIC_NUMBER);
		serializeV1(state, out);
		return out.getCopyOfBuffer();
	}

	@Override
	public StreamingCommitterState<CommT> deserialize(
			int version,
			byte[] serialized) throws IOException {
		final DataInputDeserializer in = new DataInputDeserializer(serialized);
		if (version == 1) {
			validateMagicNumber(in);
			return deserializeV1(in);
		}
		throw new IOException("Unrecognized version or corrupt state: " + version);
	}

	private StreamingCommitterState<CommT> deserializeV1(DataInputView in) throws IOException {
		final List<CommT> r = new ArrayList<>();
		final int committableSerializerVersion = in.readInt();
		final int numOfCommittable = in.readInt();

		for (int i = 0; i < numOfCommittable; i++) {
			final byte[] bytes = new byte[in.readInt()];
			in.readFully(bytes);
			final CommT committable = committableSerializer.deserialize(
					committableSerializerVersion,
					bytes);
			r.add(committable);
		}

		return new StreamingCommitterState<>(r);
	}

	private void serializeV1(
			StreamingCommitterState<CommT> state,
			DataOutputView dataOutputView) throws IOException {

		dataOutputView.writeInt(committableSerializer.getVersion());
		dataOutputView.writeInt(state.getCommittables().size());

		for (CommT committable : state.getCommittables()) {
			final byte[] serialized = committableSerializer.serialize(committable);
			dataOutputView.writeInt(serialized.length);
			dataOutputView.write(serialized);
		}
	}

	private static void validateMagicNumber(DataInputView in) throws IOException {
		final int magicNumber = in.readInt();
		if (magicNumber != MAGIC_NUMBER) {
			throw new IOException(String.format(
					"Corrupt data: Unexpected magic number %08X",
					magicNumber));
		}
	}
}
