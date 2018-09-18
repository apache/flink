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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A {@code TypeSerializerConfigSnapshot} is a point-in-time view of a {@link TypeSerializer's} configuration.
 * The configuration snapshot of a serializer is persisted within checkpoints
 * as a single source of meta information about the schema of serialized data in the checkpoint.
 * This serves three purposes:
 *
 * <ul>
 *   <li><strong>Capturing serializer parameters and schema:</strong> a serializer's configuration snapshot
 *   represents information about the parameters, state, and schema of a serializer.
 *   This is explained in more detail below.</li>
 *
 *   <li><strong>Compatibility checks for new serializers:</strong> when new serializers are available,
 *   they need to be checked whether or not they are compatible to read the data written by the previous serializer.
 *   This is performed by providing the new serializer to the correspondibng serializer configuration
 *   snapshots in checkpoints.</li>
 *
 *   <li><strong>Factory for a read serializer when schema conversion is required:<strong> in the case that new
 *   serializers are not compatible to read previous data, a schema conversion process executed across all data
 *   is required before the new serializer can be continued to be used. This conversion process requires a compatible
 *   read serializer to restore serialized bytes as objects, and then written back again using the new serializer.
 *   In this scenario, the serializer configuration snapshots in checkpoints doubles as a factory for the read
 *   serializer of the conversion process.</li>
 * </ul>
 *
 * <h2>Serializer Configuration and Schema</h2>
 *
 * <p>Since serializer configuration snapshots needs to be used to ensure serialization compatibility
 * for the same managed state as well as serving as a factory for compatible read serializers, the configuration
 * snapshot should encode sufficient information about:
 *
 * <ul>
 *   <li><strong>Parameter settings of the serializer:</strong> parameters of the serializer include settings
 *   required to setup the serializer, or the state of the serializer if it is stateful. If the serializer
 *   has nested serializers, then the configuration snapshot should also contain the parameters of the nested
 *   serializers.</li>
 *
 *   <li><strong>Serialization schema of the serializer:</strong> the binary format used by the serializer, or
 *   in other words, the schema of data written by the serializer.</li>
 * </ul>
 *
 * <p>NOTE: Implementations must contain the default empty nullary constructor. This is required to be able to
 * deserialize the configuration snapshot from its binary form.
 *
 * @param <T> The data type that the originating serializer of this configuration serializes.
 */
@PublicEvolving
public abstract class TypeSerializerConfigSnapshot<T> extends VersionedIOReadableWritable {

	/** The user code class loader; only relevant if this configuration instance was deserialized from binary form. */
	private ClassLoader userCodeClassLoader;

	/**
	 * The originating serializer of this configuration snapshot.
	 *
	 * TODO to allow for incrementally adapting the implementation of serializer config snapshot subclasses,
	 * TODO we currently have a base implementation for the {@link #restoreSerializer()}
	 * TODO method which simply returns this serializer instance. The serializer is written
	 * TODO and read using Java serialization as part of reading / writing the config snapshot
	 */
	private TypeSerializer<T> serializer;

	/**
	 * Creates a serializer using this configuration, that is capable of reading data
	 * written by the serializer described by this configuration.
	 *
	 * @return the restored serializer.
	 */
	public TypeSerializer<T> restoreSerializer() {
		// TODO this implementation is only a placeholder; the intention is to have no default implementation
		return serializer;
	}

	/**
	 * Set the originating serializer of this configuration snapshot.
	 *
	 * TODO this method is a temporary workaround to inject the serializer instance to
	 * TODO be returned by the restoreSerializer() method.
	 */
	@Internal
	public final void setSerializer(TypeSerializer<T> serializer) {
		this.serializer = Preconditions.checkNotNull(serializer);
	}

	/**
	 * Checks whether a new serializer is compatible to read data written be the originating serializer of this
	 * config snapshot; i.e. whether or not a new serializer is compatible with the previous serializer.
	 *
	 * @param newSerializer the new serializer to check against for schema compatibility.
	 *
	 * @return the resolve schema compatibility result.
	 */
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<?> newSerializer) {
		@SuppressWarnings("unchecked")
		TypeSerializer<T> castedSerializer = ((TypeSerializer<T>) newSerializer);

		if (castedSerializer.ensureCompatibility(this).isRequiresMigration()) {
			return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
		} else {
			return TypeSerializerSchemaCompatibility.compatibleAfterReconfiguration(castedSerializer);
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		// bump the version; we use this to know that there is a serializer to read as part of the config
		out.writeInt(getVersion() + 1);

		TypeSerializerSerializationUtil.writeSerializer(out, this.serializer);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		if (getReadVersion() == getVersion() + 1) {
			this.serializer = TypeSerializerSerializationUtil.tryReadSerializer(in, getUserCodeClassLoader(), true);
		}
	}

	/**
	 * Set the user code class loader.
	 * Only relevant if this configuration instance was deserialized from binary form.
	 *
	 * <p>This method is not part of the public user-facing API, and cannot be overriden.
	 *
	 * @param userCodeClassLoader user code class loader.
	 */
	@Internal
	public final void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	/**
	 * Returns the user code class loader.
	 * Only relevant if this configuration instance was deserialized from binary form.
	 *
	 * @return the user code class loader
	 */
	@Internal
	public final ClassLoader getUserCodeClassLoader() {
		return userCodeClassLoader;
	}

	public abstract boolean equals(Object obj);

	public abstract int hashCode();
}
