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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for serialization of {@link TypeSerializerConfigSnapshot}.
 */
public class TypeSerializerConfigSnapshotSerializationUtil {

	/**
	 * Writes a {@link TypeSerializerConfigSnapshot} to the provided data output view.
	 *
	 * <p>It is written with a format that can be later read again using
	 * {@link #readSerializerConfigSnapshot(DataInputView, ClassLoader)}.
	 *
	 * @param out the data output view
	 * @param serializerConfigSnapshot the serializer configuration snapshot to write
	 *
	 * @throws IOException
	 */
	public static <T> void writeSerializerConfigSnapshot(
		DataOutputView out,
		TypeSerializerConfigSnapshot<T> serializerConfigSnapshot,
		TypeSerializer<T> serializer) throws IOException {

		new TypeSerializerConfigSnapshotSerializationProxy<>(serializerConfigSnapshot, serializer).write(out);
	}

	/**
	 * Reads from a data input view a {@link TypeSerializerConfigSnapshot} that was previously
	 * written using {@link TypeSerializerConfigSnapshotSerializationUtil#writeSerializerConfigSnapshot(DataOutputView, TypeSerializerConfigSnapshot, TypeSerializer)}.
	 *
	 * @param in the data input view
	 * @param userCodeClassLoader the user code class loader to use
	 *
	 * @return the read serializer configuration snapshot
	 *
	 * @throws IOException
	 */
	public static TypeSerializerConfigSnapshot readSerializerConfigSnapshot(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		final TypeSerializerConfigSnapshotSerializationProxy proxy = new TypeSerializerConfigSnapshotSerializationProxy(userCodeClassLoader);
		proxy.read(in);

		return proxy.getSerializerConfigSnapshot();
	}

	/**
	 * Reads from a data input view multiple {@link TypeSerializerConfigSnapshot}s that was previously
	 * written using {@link TypeSerializerConfigSnapshotSerializationUtil#writeSerializerConfigSnapshot(DataOutputView, TypeSerializerConfigSnapshot, TypeSerializer)}.
	 *
	 * @param in the data input view
	 * @param userCodeClassLoader the user code class loader to use
	 *
	 * @return the read serializer configuration snapshots
	 *
	 * @throws IOException
	 */
	public static List<TypeSerializerConfigSnapshot<?>> readSerializerConfigSnapshots(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		int numFields = in.readInt();
		final List<TypeSerializerConfigSnapshot<?>> serializerConfigSnapshots = new ArrayList<>(numFields);

		TypeSerializerConfigSnapshotSerializationProxy proxy;
		for (int i = 0; i < numFields; i++) {
			proxy = new TypeSerializerConfigSnapshotSerializationProxy(userCodeClassLoader);
			proxy.read(in);
			serializerConfigSnapshots.add(proxy.getSerializerConfigSnapshot());
		}

		return serializerConfigSnapshots;
	}

	/**
	 * Utility serialization proxy for a {@link TypeSerializerConfigSnapshot}.
	 */
	static final class TypeSerializerConfigSnapshotSerializationProxy<T> extends VersionedIOReadableWritable {

		private static final int VERSION = 1;

		private ClassLoader userCodeClassLoader;
		private TypeSerializerConfigSnapshot<T> serializerConfigSnapshot;
		private TypeSerializer<T> serializer;

		TypeSerializerConfigSnapshotSerializationProxy(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		}

		TypeSerializerConfigSnapshotSerializationProxy(
			TypeSerializerConfigSnapshot<T> serializerConfigSnapshot,
			TypeSerializer<T> serializer) {
			this.serializerConfigSnapshot = Preconditions.checkNotNull(serializerConfigSnapshot);
			this.serializer = Preconditions.checkNotNull(serializer);
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			// config snapshot class, so that we can re-instantiate the
			// correct type of config snapshot instance when deserializing
			out.writeUTF(serializerConfigSnapshot.getClass().getName());

			// TODO this is a temporary workaround until all serializer config snapshot classes in Flink
			// TODO have properly implemented the restore serializer factory methods
			serializerConfigSnapshot.setSerializer(serializer);

			// the actual configuration parameters
			serializerConfigSnapshot.write(out);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			String serializerConfigClassname = in.readUTF();
			Class<? extends TypeSerializerConfigSnapshot> serializerConfigSnapshotClass;
			try {
				serializerConfigSnapshotClass = (Class<? extends TypeSerializerConfigSnapshot>)
					Class.forName(serializerConfigClassname, true, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(
					"Could not find requested TypeSerializerConfigSnapshot class "
						+ serializerConfigClassname +  " in classpath.", e);
			}

			serializerConfigSnapshot = InstantiationUtil.instantiate(serializerConfigSnapshotClass);
			serializerConfigSnapshot.setUserCodeClassLoader(userCodeClassLoader);
			serializerConfigSnapshot.read(in);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		TypeSerializerConfigSnapshot getSerializerConfigSnapshot() {
			return serializerConfigSnapshot;
		}
	}
}
