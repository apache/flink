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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

/**
 * A utility {@link TypeSerializerConfigSnapshot} that is used for backwards compatibility purposes.
 *
 * <p>In older versions of Flink (<= 1.6), we used to write state serializers into checkpoints, along
 * with the serializer's configuration snapshot. Since 1.7.0, we no longer wrote the serializers, but
 * instead used the configuration snapshot as a factory to instantiate serializers for restoring state.
 * However, since some outdated implementations of configuration snapshots did not contain sufficient
 * information to serve as a factory, the backwards compatible path for restoring from these older
 * savepoints would be to just use the written serializer.
 *
 * <p>Therefore, when restoring from older savepoints which still contained both the config snapshot
 * and the serializer, they are both wrapped within this utility class. When the caller intends
 * to instantiate a restore serializer, we simply return the wrapped serializer instance.
 *
 * @param <T> the data type that the wrapped serializer instance serializes.
 */
@Internal
public class BackwardsCompatibleConfigSnapshot<T> extends TypeSerializerConfigSnapshot<T> {

	/**
	 * The actual serializer config snapshot. This may be {@code null} when reading a
	 * savepoint from Flink <= 1.2.
	 */
	@Nullable
	private TypeSerializerConfigSnapshot<?> wrappedConfigSnapshot;

	/**
	 * The serializer instance written in savepoints.
	 */
	@Nonnull
	private TypeSerializer<T> serializerInstance;

	public BackwardsCompatibleConfigSnapshot(
			@Nullable TypeSerializerConfigSnapshot<?> wrappedConfigSnapshot,
			TypeSerializer<T> serializerInstance) {

		this.wrappedConfigSnapshot = wrappedConfigSnapshot;
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

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<?> newSerializer) {
		if (wrappedConfigSnapshot != null) {
			return (TypeSerializerSchemaCompatibility<T>) wrappedConfigSnapshot.resolveSchemaCompatibility(newSerializer);
		} else {
			// if there is no configuration snapshot to check against,
			// then we can only assume that the new serializer is compatible as is
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}
	}

	@Override
	public int hashCode() {
		int result = (wrappedConfigSnapshot != null) ? wrappedConfigSnapshot.hashCode() : 0;
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

		return Objects.equals(that.wrappedConfigSnapshot, wrappedConfigSnapshot)
			&& that.serializerInstance.equals(serializerInstance);
	}

	@VisibleForTesting
	public TypeSerializerConfigSnapshot<?> getWrappedConfigSnapshot() {
		return wrappedConfigSnapshot;
	}
}
