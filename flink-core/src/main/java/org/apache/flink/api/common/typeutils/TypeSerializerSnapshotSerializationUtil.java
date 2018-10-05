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

import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;

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
	 *
	 * @throws IOException
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
	 *
	 * @throws IOException
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

	/**
	 * Utility serialization proxy for a {@link TypeSerializerSnapshot}.
	 */
	static final class TypeSerializerSnapshotSerializationProxy<T> extends VersionedIOReadableWritable {

		private static final int VERSION = 2;

		private ClassLoader userCodeClassLoader;
		private TypeSerializerSnapshot<T> serializerSnapshot;
		private TypeSerializer<T> serializer;

		TypeSerializerSnapshotSerializationProxy(
			ClassLoader userCodeClassLoader,
			@Nullable TypeSerializer<T> existingPriorSerializer) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
			this.serializer = existingPriorSerializer;
		}

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
		 *     <li>1. Serializer snapshot classname (UTF).</li>
		 *     <li>2. The originating serializer of the snapshot, if any, written via Java serialization.
		 *         Presence of the serializer is indicated by a flag (boolean -> TypeSerializer).</li>
		 *     <li>3. The version of the serializer snapshot's binary format.</li>
		 *     <li>4. The actual serializer snapshot.</li>
		 * </ul>
		 */
		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			// config snapshot class, so that we can re-instantiate the
			// correct type of config snapshot instance when deserializing
			out.writeUTF(serializerSnapshot.getClass().getName());

			if (serializerSnapshot instanceof TypeSerializerConfigSnapshot) {
				// backwards compatible path, where the serializer snapshot is still using the
				// deprecated interface; the originating serializer needs to be written to the byte stream
				out.writeBoolean(true);
				@SuppressWarnings("unchecked")
				TypeSerializerConfigSnapshot<T> legacySerializerSnapshot = (TypeSerializerConfigSnapshot<T>) serializerSnapshot;
				TypeSerializerSerializationUtil.writeSerializer(out, serializer);

				// TypeSerializerConfigSnapshot includes the version number implicitly when it is written
				legacySerializerSnapshot.write(out);
			} else {
				out.writeBoolean(false);

				out.writeInt(serializerSnapshot.getCurrentVersion());
				serializerSnapshot.write(out);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			String serializerConfigClassname = in.readUTF();
			Class<? extends TypeSerializerSnapshot> serializerConfigSnapshotClass;
			try {
				serializerConfigSnapshotClass = (Class<? extends TypeSerializerSnapshot>)
					Class.forName(serializerConfigClassname, true, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(
					"Could not find requested TypeSerializerConfigSnapshot class "
						+ serializerConfigClassname +  " in classpath.", e);
			}

			serializerSnapshot = InstantiationUtil.instantiate(serializerConfigSnapshotClass);

			if (getReadVersion() >= 2) {
				// Flink version after 1.7

				boolean containsPriorSerializer = in.readBoolean();

				TypeSerializer<T> priorSerializer = (containsPriorSerializer)
					? TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader, true)
					: null;

				if (serializerSnapshot instanceof TypeSerializerConfigSnapshot) {
					if (priorSerializer != null) {
						((TypeSerializerConfigSnapshot<T>) serializerSnapshot).setPriorSerializer(priorSerializer);
						((TypeSerializerConfigSnapshot<T>) serializerSnapshot).setUserCodeClassLoader(userCodeClassLoader);
						((TypeSerializerConfigSnapshot<T>) serializerSnapshot).read(in);
					} else {
						// this occurs if the user changed a TypeSerializerSnapshot to the
						// legacy TypeSerializerConfigSnapshot, which isn't supported.
						throw new IOException("Cannot read a legacy TypeSerializerConfigSnapshot without the prior serializer present. ");
					}
				} else {
					int readVersion = in.readInt();
					serializerSnapshot.read(readVersion, in, userCodeClassLoader);
				}
			} else {
				// Flink version before 1.7.x, and after 1.3.x

				if (serializerSnapshot instanceof TypeSerializerConfigSnapshot) {
					((TypeSerializerConfigSnapshot<T>) serializerSnapshot).setPriorSerializer(this.serializer);
					((TypeSerializerConfigSnapshot<T>) serializerSnapshot).setUserCodeClassLoader(userCodeClassLoader);
					((TypeSerializerConfigSnapshot<T>) serializerSnapshot).read(in);
				} else {
					int readVersion = in.readInt();
					serializerSnapshot.read(readVersion, in, userCodeClassLoader);
				}
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
	}
}
