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
 * A utility {@link TypeSerializerConfigSnapshot} that is used for backwards compatibility purposes.
 *
 * @param <T> the data type that the wrapped serializer instance serializes.
 */
@Internal
public class BackwardsCompatibleConfigSnapshot<T> extends TypeSerializerConfigSnapshot<T> {

	private TypeSerializerConfigSnapshot<?> wrappedConfigSnapshot;

	private TypeSerializer<T> serializerInstance;

	public BackwardsCompatibleConfigSnapshot(
			TypeSerializerConfigSnapshot<?> wrappedConfigSnapshot,
			TypeSerializer<T> serializerInstance) {

		this.wrappedConfigSnapshot = Preconditions.checkNotNull(wrappedConfigSnapshot);
		this.serializerInstance = Preconditions.checkNotNull(serializerInstance);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		throw new UnsupportedOperationException(
			"This is a dummy config snapshot used only for backwards compatibility.");
	}

	@Override
	public void read(DataInputView in) throws IOException {
		throw new UnsupportedOperationException(
			"This is a dummy config snapshot used only for backwards compatibility.");
	}

	@Override
	public int getVersion() {
		throw new UnsupportedOperationException(
			"This is a dummy config snapshot used only for backwards compatibility.");
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		return serializerInstance;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<?> newSerializer) {
		return (TypeSerializerSchemaCompatibility<T>) wrappedConfigSnapshot.resolveSchemaCompatibility(newSerializer);
	}

	public TypeSerializerConfigSnapshot<?> getWrappedConfigSnapshot() {
		return wrappedConfigSnapshot;
	}

	@Override
	public int hashCode() {
		int result = wrappedConfigSnapshot.hashCode();
		result = 31 * result + serializerInstance.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BackwardsCompatibleConfigSnapshot<?> that = (BackwardsCompatibleConfigSnapshot<?>) o;

		return that.wrappedConfigSnapshot.equals(wrappedConfigSnapshot)
			&& that.serializerInstance.equals(serializerInstance);
	}
}
