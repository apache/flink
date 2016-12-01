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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TypeSerializerSerializationProxy<T> extends VersionedIOReadableWritable {

	public static final int VERSION = 1;
	private static final Logger LOG = LoggerFactory.getLogger(TypeSerializerSerializationProxy.class);

	private ClassLoader userClassLoader;

	private TypeSerializer<T> typeSerializer;

	private String typeSerializerClassName;
	private int typeSerializerVersion;

	public TypeSerializerSerializationProxy(ClassLoader userClassLoader) {
		this.userClassLoader = userClassLoader;
	}

	public TypeSerializerSerializationProxy(TypeSerializer<T> typeSerializer) {
		this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
		this.typeSerializerVersion = typeSerializer.getVersion();
		this.typeSerializerClassName = typeSerializer.getCanonicalClassName();
	}

	public TypeSerializer<T> getTypeSerializer() {
		return typeSerializer;
	}

	public String getTypeSerializerClassName() {
		return typeSerializerClassName;
	}

	public int getTypeSerializerVersion() {
		return typeSerializerVersion;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		out.writeUTF(typeSerializerClassName);
		out.writeInt(typeSerializerVersion);

		if (typeSerializer instanceof ClassNotFoundDummyTypeSerializer) {
			ClassNotFoundDummyTypeSerializer<T> dummyTypeSerializer =
					(ClassNotFoundDummyTypeSerializer<T>) this.typeSerializer;

			byte[] serializerBytes = dummyTypeSerializer.getActualBytes();
			out.write(serializerBytes.length);
			out.write(serializerBytes);
		} else {
			// write in a way that allows the stream to recover from exceptions
			try (ByteArrayOutputStreamWithPos streamWithPos = new ByteArrayOutputStreamWithPos()) {
				InstantiationUtil.serializeObject(streamWithPos, typeSerializer);
				out.writeInt(streamWithPos.getPosition());
				out.write(streamWithPos.getBuf(), 0, streamWithPos.getPosition());
			}
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		this.typeSerializerClassName = in.readUTF();
		this.typeSerializerVersion = in.readInt();

		// read in a way that allows the stream to recover from exceptions
		int serializerBytes = in.readInt();
		byte[] buffer = new byte[serializerBytes];
		in.read(buffer);
		try {
			typeSerializer = InstantiationUtil.deserializeObject(buffer, userClassLoader);
		} catch (ClassNotFoundException e) {
			// we create a dummy so that all the information is not lost when we get a new checkpoint before receiving
			// a proper typeserializer from the user
			typeSerializer =
					new ClassNotFoundDummyTypeSerializer<>(typeSerializerClassName, typeSerializerVersion, buffer);
			LOG.warn("Could not find requested TypeSerializer class in classpath. Created dummy.", e);
		}
	}

	/**
	 * Dummy TypeSerializer to avoid that data is lost when checkpointing again a serializer for which we encountered
	 * a {@link ClassNotFoundException}.
	 */
	static final class ClassNotFoundDummyTypeSerializer<T> extends TypeSerializer<T> {

		private final String actualCanonicalClassName;
		private final int actualVersion;
		private final byte[] actualBytes;

		public ClassNotFoundDummyTypeSerializer(
				String actualCanonicalClassName, int actualVersion, byte[] actualBytes) {

			this.actualCanonicalClassName = Preconditions.checkNotNull(actualCanonicalClassName);
			this.actualBytes = Preconditions.checkNotNull(actualBytes);
			this.actualVersion = actualVersion;
		}

		public String getActualCanonicalClassName() {
			return actualCanonicalClassName;
		}

		public int getActualVersion() {
			return actualVersion;
		}

		public byte[] getActualBytes() {
			return actualBytes;
		}

		@Override
		public boolean isImmutableType() {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public TypeSerializer<T> duplicate() {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public T createInstance() {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public T copy(T from) {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public T copy(T from, T reuse) {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public int getLength() {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public void serialize(T record, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public T deserialize(DataInputView source) throws IOException {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public T deserialize(T reuse, DataInputView source) throws IOException {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public boolean canEqual(Object obj) {
			throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
		}

		@Override
		public int getVersion() {
			return getActualVersion();
		}

		@Override
		public String getCanonicalClassName() {
			return actualCanonicalClassName;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			ClassNotFoundDummyTypeSerializer<?> that = (ClassNotFoundDummyTypeSerializer<?>) o;

			if (actualVersion != that.actualVersion) {
				return false;
			}
			return actualCanonicalClassName.equals(that.actualCanonicalClassName);
		}

		@Override
		public int hashCode() {
			int result = actualCanonicalClassName.hashCode();
			result = 31 * result + actualVersion;
			return result;
		}
	}
}