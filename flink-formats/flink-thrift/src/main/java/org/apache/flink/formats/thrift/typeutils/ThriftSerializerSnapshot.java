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

package org.apache.flink.formats.thrift.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.thrift.ThriftCodeGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@code Avro} specific implementation of a {@link TypeSerializerSnapshot}.
 *
 * @param <T> The data type that the originating serializer of this configuration serializes.
 */
public class ThriftSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {
	private static final Logger LOG = LoggerFactory.getLogger(ThriftSerializerSnapshot.class);

	private Class<T> runtimeType;
	private ThriftCodeGenerator codeGenerator;

	@SuppressWarnings("WeakerAccess")
	public ThriftSerializerSnapshot() {
		// this constructor is used when restoring from a checkpoint.
	}

	ThriftSerializerSnapshot(Class<T> runtimeType, ThriftCodeGenerator codeGenerator) {
		this.runtimeType = runtimeType;
		this.codeGenerator = codeGenerator;
	}

	@Override
	public int getCurrentVersion() {
		return 2;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		checkNotNull(runtimeType);
		out.writeUTF(runtimeType.getName());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		final String thriftClassName = in.readUTF();
		try {
			this.runtimeType = (Class<T>) Class.forName(thriftClassName);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
		if (!(newSerializer instanceof ThriftSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
		ThriftSerializer<?> newThriftSerializer = (ThriftSerializer<?>) newSerializer;
		return runtimeType.getName().equals(newThriftSerializer.getThriftClassName())
			? TypeSerializerSchemaCompatibility.compatibleAsIs()
			: TypeSerializerSchemaCompatibility.incompatible();
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		checkNotNull(runtimeType);
		return new ThriftSerializer(runtimeType, codeGenerator);
	}

	@SuppressWarnings("unchecked")
	@Nonnull
	private static <T> Class<T> findClassOrThrow(ClassLoader userCodeClassLoader, String className) {
		try {
			Class<?> runtimeTarget = Class.forName(className, false, userCodeClassLoader);
			return (Class<T>) runtimeTarget;
		} catch (ClassNotFoundException e) {
			LOG.error("Failed to find class {}", className, e);
			throw new IllegalStateException(""
				+ "Unable to find the class '" + className + "' which is used to deserialize "
				+ "the elements of this serializer. "
				+ "Were the class was moved or renamed?", e);
		}
	}
}
