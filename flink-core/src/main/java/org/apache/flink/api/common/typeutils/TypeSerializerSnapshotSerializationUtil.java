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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility methods for serialization of {@link TypeSerializerSnapshot}.
 */
public class TypeSerializerSnapshotSerializationUtil {

	/**
	 * Writes a {@link TypeSerializerSnapshot} to the provided data output view.
	 *
	 * <p>It is written with a format that can be later read again using
	 * {@link #readSerializerSnapshot(DataInputView, ClassLoader, TypeSerializer)}.
	 *
	 * @param out the data output view
	 * @param serializerSnapshot the serializer configuration snapshot to write
	 * @param serializer the prior serializer. This needs to be written of the serializer snapshot
	 *                   if the serializer snapshot is still the legacy {@link TypeSerializerConfigSnapshot}.
	 */
	public static <T> void writeSerializerSnapshot(
		DataOutputView out,
		TypeSerializerSnapshot<T> serializerSnapshot,
		TypeSerializer<T> serializer) throws IOException {

		new TypeSerializerSnapshotSerializationProxy<>(serializerSnapshot, serializer).write(out);
	}

	/**
	 * Reads from a data input view a {@link TypeSerializerSnapshot} that was previously
	 * written using {@link TypeSerializerSnapshotSerializationUtil#writeSerializerSnapshot(DataOutputView, TypeSerializerSnapshot, TypeSerializer)}.
	 *
	 * @param in the data input view
	 * @param userCodeClassLoader the user code class loader to use
	 * @param existingPriorSerializer the prior serializer. This would only be non-null if we are
	 *                                restoring from a snapshot taken with Flink version <= 1.6.
	 *
	 * @return the read serializer configuration snapshot
	 */
	public static <T> TypeSerializerSnapshot<T> readSerializerSnapshot(
			DataInputView in,
			ClassLoader userCodeClassLoader,
			@Nullable TypeSerializer<T> existingPriorSerializer) throws IOException {

		final TypeSerializerSnapshotSerializationProxy<T> proxy =
			new TypeSerializerSnapshotSerializationProxy<>(userCodeClassLoader, existingPriorSerializer);
		proxy.read(in);

		return proxy.getSerializerSnapshot();
	}


	public static <T> TypeSerializerSnapshot<T> readAndInstantiateSnapshotClass(DataInputView in, ClassLoader cl) throws IOException {
		Class<TypeSerializerSnapshot<T>> clazz =
				InstantiationUtil.resolveClassByName(in, cl, TypeSerializerSnapshot.class);

		return InstantiationUtil.instantiate(clazz);
	}

	/**
	 * Utility serialization proxy for a {@link TypeSerializerSnapshot}.
	 */
	static final class TypeSerializerSnapshotSerializationProxy<T> extends VersionedIOReadableWritable {

		private static final int VERSION = 2;

		private ClassLoader userCodeClassLoader;
		private TypeSerializerSnapshot<T> serializerSnapshot;
		@Nullable private TypeSerializer<T> serializer;

		/**
		 * Constructor for reading serializers.
		 */
		TypeSerializerSnapshotSerializationProxy(
			ClassLoader userCodeClassLoader,
			@Nullable TypeSerializer<T> existingPriorSerializer) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
			this.serializer = existingPriorSerializer;
		}

		/**
		 * Constructor for writing out serializers.
		 */
		TypeSerializerSnapshotSerializationProxy(
			TypeSerializerSnapshot<T> serializerConfigSnapshot,
			TypeSerializer<T> serializer) {
			this.serializerSnapshot = Preconditions.checkNotNull(serializerConfigSnapshot);
			this.serializer = Preconditions.checkNotNull(serializer);
		}

		/**
		 * Binary format layout of a written serializer snapshot is as follows:
		 *
		 * <ul>
		 *     <li>1. Format version of this util.</li>
		 *     <li>2. Name of the TypeSerializerSnapshot class.</li>
		 *     <li>3. The version of the TypeSerializerSnapshot's binary format.</li>
		 *     <li>4. The actual serializer snapshot data.</li>
		 * </ul>
		 */
		@SuppressWarnings("deprecation")
		@Override
		public void write(DataOutputView out) throws IOException {
			setSerializerForWriteIfOldPath(serializerSnapshot, serializer);

			// write the format version of this utils format
			super.write(out);

			TypeSerializerSnapshot.writeVersionedSnapshot(out, serializerSnapshot);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void read(DataInputView in) throws IOException {
			// read version
			super.read(in);
			final int version = getReadVersion();

			switch (version) {
				case 2:
					serializerSnapshot = deserializeV2(in, userCodeClassLoader);
					break;
				case 1:
					serializerSnapshot = deserializeV1(in, userCodeClassLoader, serializer);
					break;
				default:
					throw new IOException("Unrecognized version for TypeSerializerSnapshot format: " + version);
			}
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public int[] getCompatibleVersions() {
			return new int[]{VERSION, 1};
		}

		TypeSerializerSnapshot<T> getSerializerSnapshot() {
			return serializerSnapshot;
		}

		/**
		 * Deserialization path for Flink versions 1.7+.
		 */
		@VisibleForTesting
		static <T> TypeSerializerSnapshot<T> deserializeV2(DataInputView in, ClassLoader cl) throws IOException {
			return TypeSerializerSnapshot.readVersionedSnapshot(in, cl);
		}

		/**
		 * Deserialization path for Flink versions in [1.4, 1.6].
		 */
		@VisibleForTesting
		@SuppressWarnings("deprecation")
		static <T> TypeSerializerSnapshot<T> deserializeV1(
				DataInputView in,
				ClassLoader cl,
				@Nullable TypeSerializer<T> serializer) throws IOException {

			TypeSerializerSnapshot<T> snapshot = readAndInstantiateSnapshotClass(in, cl);

			// if the snapshot was created before Flink 1.7, we need to distinguish the following cases:
			//   - old snapshot type that needs serializer from the outside
			//   - new snapshot type that understands the old format and can produce a restore serializer from it
			if (snapshot instanceof TypeSerializerConfigSnapshot) {
				TypeSerializerConfigSnapshot<T> oldTypeSnapshot = (TypeSerializerConfigSnapshot<T>) snapshot;
				oldTypeSnapshot.setPriorSerializer(serializer);
				oldTypeSnapshot.setUserCodeClassLoader(cl);
				oldTypeSnapshot.read(in);
			}
			else {
				// new type, simple case
				int readVersion = in.readInt();
				snapshot.readSnapshot(readVersion, in, cl);
			}

			return snapshot;
		}

		@SuppressWarnings("deprecation")
		private static <T> void setSerializerForWriteIfOldPath(
				TypeSerializerSnapshot<T> serializerSnapshot,
				TypeSerializer<T> serializer) {

			// for compatibility with non-upgraded serializers, put the serializer into the
			// config snapshot if it of the old version
			if (serializerSnapshot instanceof TypeSerializerConfigSnapshot) {
				checkState(serializer != null);

				((TypeSerializerConfigSnapshot<T>) serializerSnapshot).setPriorSerializer(serializer);
			}
		}
	}
}
