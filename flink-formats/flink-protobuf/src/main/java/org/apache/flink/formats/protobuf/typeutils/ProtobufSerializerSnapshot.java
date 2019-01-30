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

package org.apache.flink.formats.protobuf.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@code Protobuf} specific implementation of a {@link TypeSerializerSnapshot}.
 *
 * @param <T> The data type that the originating serializer of this configuration serializes.
 */
public class ProtobufSerializerSnapshot<T extends Message> implements TypeSerializerSnapshot<T> {

	private Class<T> runtimeType;

	private final ExecutionConfig executionConfig = new ExecutionConfig();

	@SuppressWarnings("WeakerAccess")
	public ProtobufSerializerSnapshot() {
		// this constructor is used when restoring from a checkpoint.
	}

	public ProtobufSerializerSnapshot(Class<T> runtimeType) {
		this.runtimeType = runtimeType;
	}

	@Override
	public int getCurrentVersion() {
		return 1;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		Preconditions.checkNotNull(runtimeType);

		out.writeUTF(runtimeType.getName());
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		final String previousRuntimeTypeName = in.readUTF();
		this.runtimeType = findClassOrThrow(userCodeClassLoader, previousRuntimeTypeName);
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		checkNotNull(runtimeType);

		// Restore serializer means we already use ProtobufSerializer to snapshot data,
		// no need to consider keeping backward compatibility for kryo serializer.
		return new ProtobufSerializer<>(runtimeType, executionConfig);
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
		if (!(newSerializer instanceof ProtobufSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
		ProtobufSerializer<?> protobufSerializer = (ProtobufSerializer<?>) newSerializer;
		// TODO support schema evolution
		if (runtimeType.equals(protobufSerializer.getType())) {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}
		return TypeSerializerSchemaCompatibility.incompatible();
	}

	@SuppressWarnings("unchecked")
	@Nonnull
	private static <T> Class<T> findClassOrThrow(ClassLoader userCodeClassLoader, String className) {
		try {
			Class<?> runtimeTarget = Class.forName(className, false, userCodeClassLoader);
			return (Class<T>) runtimeTarget;
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException(""
				+ "Unable to find the class '" + className + "' which is used to deserialize "
				+ "the elements of this serializer. "
				+ "Were the class was moved or renamed?", e);
		}
	}
}
