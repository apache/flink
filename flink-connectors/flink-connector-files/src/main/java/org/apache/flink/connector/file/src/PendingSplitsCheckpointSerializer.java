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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer for the {@link PendingSplitsCheckpoint}.
 */
@PublicEvolving
public final class PendingSplitsCheckpointSerializer<T extends FileSourceSplit>
		implements SimpleVersionedSerializer<PendingSplitsCheckpoint<T>> {

	private static final int VERSION = 1;

	private static final int VERSION_1_MAGIC_NUMBER = 0xDEADBEEF;

	private final SimpleVersionedSerializer<T> splitSerializer;

	public PendingSplitsCheckpointSerializer(SimpleVersionedSerializer<T> splitSerializer) {
		this.splitSerializer = checkNotNull(splitSerializer);
	}

	// ------------------------------------------------------------------------

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public byte[] serialize(PendingSplitsCheckpoint<T> checkpoint) throws IOException {
		checkArgument(checkpoint.getClass() == PendingSplitsCheckpoint.class,
				"Cannot serialize subclasses of PendingSplitsCheckpoint");

		// optimization: the splits lazily cache their own serialized form
		if (checkpoint.serializedFormCache != null) {
			return checkpoint.serializedFormCache;
		}

		final SimpleVersionedSerializer<T> splitSerializer = this.splitSerializer; // stack cache
		final Collection<T> splits = checkpoint.getSplits();
		final Collection<Path> processedPaths = checkpoint.getAlreadyProcessedPaths();

		final ArrayList<byte[]> serializedSplits = new ArrayList<>(splits.size());
		final ArrayList<byte[]> serializedPaths = new ArrayList<>(processedPaths.size());

		int totalLen = 16;	// four ints: magic, version of split serializer, count splits, count paths

		for (T split : splits) {
			final byte[] serSplit = splitSerializer.serialize(split);
			serializedSplits.add(serSplit);
			totalLen += serSplit.length + 4; // 4 bytes for the length field
		}

		for (Path path : processedPaths) {
			final byte[] serPath = path.toString().getBytes(StandardCharsets.UTF_8);
			serializedPaths.add(serPath);
			totalLen += serPath.length + 4; // 4 bytes for the length field
		}

		final byte[] result = new byte[totalLen];
		final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
		byteBuffer.putInt(VERSION_1_MAGIC_NUMBER);
		byteBuffer.putInt(splitSerializer.getVersion());
		byteBuffer.putInt(serializedSplits.size());
		byteBuffer.putInt(serializedPaths.size());

		for (byte[] splitBytes : serializedSplits) {
			byteBuffer.putInt(splitBytes.length);
			byteBuffer.put(splitBytes);
		}

		for (byte[] pathBytes : serializedPaths) {
			byteBuffer.putInt(pathBytes.length);
			byteBuffer.put(pathBytes);
		}

		assert byteBuffer.remaining() == 0;

		// optimization: cache the serialized from, so we avoid the byte work during repeated serialization
		checkpoint.serializedFormCache = result;

		return result;
	}

	@Override
	public PendingSplitsCheckpoint<T> deserialize(int version, byte[] serialized) throws IOException {
		if (version == 1) {
			return deserializeV1(serialized);
		}
		throw new IOException("Unknown version: " + version);
	}

	private PendingSplitsCheckpoint<T> deserializeV1(byte[] serialized) throws IOException {
		final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

		final int magic = bb.getInt();
		if (magic != VERSION_1_MAGIC_NUMBER) {
			throw new IOException(String.format("Invalid magic number for PendingSplitsCheckpoint. " +
				"Expected: %X , found %X", VERSION_1_MAGIC_NUMBER, magic));
		}

		final int splitSerializerVersion = bb.getInt();
		final int numSplits = bb.getInt();
		final int numPaths = bb.getInt();

		final SimpleVersionedSerializer<T> splitSerializer = this.splitSerializer; // stack cache
		final ArrayList<T> splits = new ArrayList<>(numSplits);
		final ArrayList<Path> paths = new ArrayList<>(numPaths);

		for (int remaining = numSplits; remaining > 0; remaining--) {
			final byte[] bytes = new byte[bb.getInt()];
			bb.get(bytes);
			final T split = splitSerializer.deserialize(splitSerializerVersion, bytes);
			splits.add(split);
		}

		for (int remaining = numPaths; remaining > 0; remaining--) {
			final byte[] bytes = new byte[bb.getInt()];
			bb.get(bytes);
			final Path path = new Path(new String(bytes, StandardCharsets.UTF_8));
			paths.add(path);
		}

		return PendingSplitsCheckpoint.reusingCollection(splits, paths);
	}
}
