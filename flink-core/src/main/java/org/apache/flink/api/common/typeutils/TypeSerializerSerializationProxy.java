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
import java.util.Arrays;

public class TypeSerializerSerializationProxy<T> extends VersionedIOReadableWritable {

	public static final int VERSION = 1;
	private static final Logger LOG = LoggerFactory.getLogger(TypeSerializerSerializationProxy.class);

	private ClassLoader userClassLoader;

	private TypeSerializer<T> typeSerializer;

	private boolean ignoreClassNotFound;

	public TypeSerializerSerializationProxy(ClassLoader userClassLoader, boolean ignoreClassNotFound) {
		this.userClassLoader = userClassLoader;
		this.ignoreClassNotFound = ignoreClassNotFound;
	}

	public TypeSerializerSerializationProxy(ClassLoader userClassLoader) {
		this(userClassLoader, false);
	}

	public TypeSerializerSerializationProxy(TypeSerializer<T> typeSerializer) {
		this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
		this.ignoreClassNotFound = false;
	}

	public TypeSerializer<T> getTypeSerializer() {
		return typeSerializer;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

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

		// read in a way that allows the stream to recover from exceptions
		int serializerBytes = in.readInt();
		byte[] buffer = new byte[serializerBytes];
		in.read(buffer);
		try {
			typeSerializer = InstantiationUtil.deserializeObject(buffer, userClassLoader);
		} catch (ClassNotFoundException e) {
			if (ignoreClassNotFound) {
				// we create a dummy so that all the information is not lost when we get a new checkpoint before receiving
				// a proper typeserializer from the user
				typeSerializer =
						new ClassNotFoundDummyTypeSerializer<>(buffer);
				LOG.warn("Could not find requested TypeSerializer class in classpath. Created dummy.", e);
			} else {
				throw new IOException("Missing class for type serializer.", e);
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TypeSerializerSerializationProxy<?> that = (TypeSerializerSerializationProxy<?>) o;

		return getTypeSerializer() != null ? getTypeSerializer().equals(that.getTypeSerializer()) : that.getTypeSerializer() == null;
	}

	@Override
	public int hashCode() {
		return getTypeSerializer() != null ? getTypeSerializer().hashCode() : 0;
	}

	public boolean isIgnoreClassNotFound() {
		return ignoreClassNotFound;
	}

	public void setIgnoreClassNotFound(boolean ignoreClassNotFound) {
		this.ignoreClassNotFound = ignoreClassNotFound;
	}

	/**
	 * Dummy TypeSerializer to avoid that data is lost when checkpointing again a serializer for which we encountered
	 * a {@link ClassNotFoundException}.
	 */
	static final class ClassNotFoundDummyTypeSerializer<T> extends TypeSerializer<T> {

		private static final long serialVersionUID = 2526330533671642711L;
		private final byte[] actualBytes;

		public ClassNotFoundDummyTypeSerializer(byte[] actualBytes) {
			this.actualBytes = Preconditions.checkNotNull(actualBytes);
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
			return false;
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

			return Arrays.equals(getActualBytes(), that.getActualBytes());
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(getActualBytes());
		}
	}
}