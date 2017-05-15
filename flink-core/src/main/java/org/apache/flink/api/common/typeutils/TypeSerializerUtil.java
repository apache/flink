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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Utility methods for {@link TypeSerializer} and {@link TypeSerializerConfigSnapshot}.
 */
@Internal
public class TypeSerializerUtil {

	/**
	 * Creates an array of {@link TypeSerializerConfigSnapshot}s taken
	 * from the provided array of {@link TypeSerializer}s.
	 *
	 * @param serializers array of type serializers.
	 *
	 * @return array of configuration snapshots taken from each serializer.
	 */
	public static TypeSerializerConfigSnapshot[] snapshotConfigurations(TypeSerializer<?>[] serializers) {
		final TypeSerializerConfigSnapshot[] configSnapshots = new TypeSerializerConfigSnapshot[serializers.length];

		for (int i = 0; i < serializers.length; i++) {
			configSnapshots[i] = serializers[i].snapshotConfiguration();
		}

		return configSnapshots;
	}

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
	public static void writeSerializerConfigSnapshot(
			DataOutputView out,
			TypeSerializerConfigSnapshot serializerConfigSnapshot) throws IOException {

		new TypeSerializerConfigSnapshotProxy(serializerConfigSnapshot).write(out);
	}

	/**
	 * Reads from a data input view a {@link TypeSerializerConfigSnapshot} that was previously
	 * written using {@link #writeSerializerConfigSnapshot(DataOutputView, TypeSerializerConfigSnapshot)}.
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

		final TypeSerializerConfigSnapshotProxy proxy = new TypeSerializerConfigSnapshotProxy(userCodeClassLoader);
		proxy.read(in);

		return proxy.getSerializerConfigSnapshot();
	}

	/**
	 * Writes multiple {@link TypeSerializerConfigSnapshot}s to the provided data output view.
	 *
	 * <p>It is written with a format that can be later read again using
	 * {@link #readSerializerConfigSnapshots(DataInputView, ClassLoader)}.
	 *
	 * @param out the data output view
	 * @param serializerConfigSnapshots the serializer configuration snapshots to write
	 *
	 * @throws IOException
	 */
	public static void writeSerializerConfigSnapshots(
			DataOutputView out,
			TypeSerializerConfigSnapshot... serializerConfigSnapshots) throws IOException {

		out.writeInt(serializerConfigSnapshots.length);

		for (TypeSerializerConfigSnapshot snapshot : serializerConfigSnapshots) {
			new TypeSerializerConfigSnapshotProxy(snapshot).write(out);
		}
	}

	/**
	 * Reads from a data input view multiple {@link TypeSerializerConfigSnapshot}s that was previously
	 * written using {@link #writeSerializerConfigSnapshot(DataOutputView, TypeSerializerConfigSnapshot)}.
	 *
	 * @param in the data input view
	 * @param userCodeClassLoader the user code class loader to use
	 *
	 * @return the read serializer configuration snapshots
	 *
	 * @throws IOException
	 */
	public static TypeSerializerConfigSnapshot[] readSerializerConfigSnapshots(
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {

		int numFields = in.readInt();
		final TypeSerializerConfigSnapshot[] serializerConfigSnapshots = new TypeSerializerConfigSnapshot[numFields];

		TypeSerializerConfigSnapshotProxy proxy;
		for (int i = 0; i < numFields; i++) {
			proxy = new TypeSerializerConfigSnapshotProxy(userCodeClassLoader);
			proxy.read(in);
			serializerConfigSnapshots[i] = proxy.getSerializerConfigSnapshot();
		}

		return serializerConfigSnapshots;
	}

	/**
	 * Utility serialization proxy for a {@link TypeSerializerConfigSnapshot}.
	 */
	static class TypeSerializerConfigSnapshotProxy extends VersionedIOReadableWritable {

		private static final int VERSION = 1;

		private ClassLoader userCodeClassLoader;
		private TypeSerializerConfigSnapshot serializerConfigSnapshot;

		TypeSerializerConfigSnapshotProxy(ClassLoader userCodeClassLoader) {
			this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		}

		TypeSerializerConfigSnapshotProxy(TypeSerializerConfigSnapshot serializerConfigSnapshot) {
			this.serializerConfigSnapshot = serializerConfigSnapshot;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			// config snapshot class, so that we can re-instantiate the
			// correct type of config snapshot instance when deserializing
			out.writeUTF(serializerConfigSnapshot.getClass().getName());

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
