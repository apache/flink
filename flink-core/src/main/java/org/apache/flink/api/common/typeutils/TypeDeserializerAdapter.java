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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A utility class that is used to bridge a {@link TypeSerializer} and {@link TypeDeserializer}.
 * It either wraps a type deserializer or serializer, and can only ever be used for deserialization
 * (i.e. only read-related methods is functional).
 *
 * <p>Methods related to deserialization are directly forwarded to the wrapped deserializer or serializer,
 * while serialization methods are masked and not intended for use.
 *
 * @param <T> The data type that the deserializer deserializes.
 */
@Internal
public final class TypeDeserializerAdapter<T> extends TypeSerializer<T> implements TypeDeserializer<T> {

	private static final long serialVersionUID = 1L;

	/** The actual wrapped deserializer or serializer instance */
	private final TypeDeserializer<T> deserializer;
	private final TypeSerializer<T> serializer;

	/**
	 * Creates a {@link TypeDeserializerAdapter} that wraps a {@link TypeDeserializer}.
	 *
	 * @param deserializer the actual deserializer to wrap.
	 */
	public TypeDeserializerAdapter(TypeDeserializer<T> deserializer) {
		this.deserializer = Preconditions.checkNotNull(deserializer);
		this.serializer = null;
	}

	/**
	 * Creates a {@link TypeDeserializerAdapter} that wraps a {@link TypeSerializer}.
	 *
	 * @param serializer the actual serializer to wrap.
	 */
	public TypeDeserializerAdapter(TypeSerializer<T> serializer) {
		this.deserializer = null;
		this.serializer = Preconditions.checkNotNull(serializer);
	}

	// --------------------------------------------------------------------------------------------
	// Forwarded deserialization related methods
	// --------------------------------------------------------------------------------------------

	public T deserialize(DataInputView source) throws IOException {
		return (deserializer != null) ? deserializer.deserialize(source) : serializer.deserialize(source);
	}

	public T deserialize(T reuse, DataInputView source) throws IOException {
		return (deserializer != null) ? deserializer.deserialize(reuse, source) : serializer.deserialize(reuse, source);
	}

	public TypeSerializer<T> duplicate() {
		return (deserializer != null) ? deserializer.duplicate() : serializer.duplicate();
	}

	public int getLength() {
		return (deserializer != null) ? deserializer.getLength() : serializer.getLength();
	}

	public boolean equals(Object obj) {
		return (deserializer != null) ? deserializer.equals(obj) : serializer.equals(obj);
	}

	public boolean canEqual(Object obj) {
		return (deserializer != null) ? deserializer.canEqual(obj) : serializer.canEqual(obj);
	}

	public int hashCode() {
		return (deserializer != null) ? deserializer.hashCode() : serializer.hashCode();
	}

	// --------------------------------------------------------------------------------------------
	// Irrelevant methods not intended for use
	// --------------------------------------------------------------------------------------------

	public boolean isImmutableType() {
		throw new UnsupportedOperationException(
			"This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
	}

	public T createInstance() {
		throw new UnsupportedOperationException(
			"This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
	}

	public T copy(T from) {
		throw new UnsupportedOperationException(
			"This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
	}

	public T copy(T from, T reuse) {
		throw new UnsupportedOperationException(
			"This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
	}

	public void serialize(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException(
			"This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
	}

	public void copy(DataInputView source, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException(
			"This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
	}

	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		throw new UnsupportedOperationException(
			"This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
	}

	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		throw new UnsupportedOperationException(
			"This is a TypeDeserializerAdapter used only for deserialization; this method should not be used.");
	}

}
