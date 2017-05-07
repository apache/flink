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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A utility class that wraps a {@link TypeDeserializer} as a {@link TypeSerializer}.
 *
 * <p>Methods related to deserialization are directly forwarded to the wrapped deserializer,
 * while serialization methods are masked and not intended for use.
 *
 * @param <T> The data type that the deserializer deserializes.
 */
public final class TypeDeserializerAdapter<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	/** The actual wrapped deserializer instance */
	private final TypeDeserializer<T> deserializer;

	/**
	 * Creates a {@link TypeSerializer} that wraps a {@link TypeDeserializer}.
	 *
	 * @param deserializer the actual deserializer to wrap.
	 */
	public TypeDeserializerAdapter(TypeDeserializer<T> deserializer) {
		this.deserializer = Preconditions.checkNotNull(deserializer);
	}

	// --------------------------------------------------------------------------------------------
	// Forwarded deserialization related methods
	// --------------------------------------------------------------------------------------------

	public T deserialize(DataInputView source) throws IOException {
		return deserializer.deserialize(source);
	}

	public T deserialize(T reuse, DataInputView source) throws IOException {
		return deserializer.deserialize(reuse, source);
	}

	public TypeSerializer<T> duplicate() {
		return deserializer.duplicate();
	}

	public int getLength() {
		return deserializer.getLength();
	}

	public boolean equals(Object obj) {
		return deserializer.equals(obj);
	}

	public boolean canEqual(Object obj) {
		return deserializer.canEqual(obj);
	}

	public int hashCode() {
		return deserializer.hashCode();
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
