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

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class bridges between the old serializer config snapshot interface (this class) and the new
 * serializer config snapshot interface ({@link TypeSerializerSnapshot}).
 *
 * <p>Serializers that create snapshots and compatibility checks with the old interfaces extends this class
 * and should migrate to extend {@code TypeSerializerSnapshot} to properly support state evolution/migration
 * and be future-proof.
 */
@PublicEvolving
@Deprecated
public abstract class TypeSerializerConfigSnapshot<T> extends VersionedIOReadableWritable implements TypeSerializerSnapshot<T> {

	/** Version / Magic number for the format that bridges between the old and new interface. */
	private static final int ADAPTER_VERSION = 0x7a53c4f0;

	/** The user code class loader; only relevant if this configuration instance was deserialized from binary form. */
	private ClassLoader userCodeClassLoader;

	/** The originating serializer of this configuration snapshot. */
	private TypeSerializer<T> serializer;

	/**
	 * Set the originating serializer of this configuration snapshot.
	 */
	@Internal
	public final void setPriorSerializer(TypeSerializer<T> serializer) {
		this.serializer = Preconditions.checkNotNull(serializer);
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

	// ----------------------------------------------------------------------------
	//  Implementation of the TypeSerializerSnapshot interface
	// ----------------------------------------------------------------------------

	@Override
	public final int getCurrentVersion() {
		return ADAPTER_VERSION;
	}

	@Override
	public final void writeSnapshot(DataOutputView out) throws IOException {
		checkState(serializer != null, "the prior serializer has not been set on this");

		// write the snapshot for a non-updated serializer.
		// this mimics the previous behavior where the TypeSerializer was
		// Java-serialized, for backwards compatibility
		TypeSerializerSerializationUtil.writeSerializer(out, serializer);

		// now delegate to the snapshots own writing code
		write(out);
	}

	@Override
	public final void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		if (readVersion != ADAPTER_VERSION) {
			throw new IOException("Wrong/unexpected version for the TypeSerializerConfigSnapshot: " + readVersion);
		}

		serializer = TypeSerializerSerializationUtil.tryReadSerializer(in, userCodeClassLoader, true);

		// now delegate to the snapshots own reading code
		setUserCodeClassLoader(userCodeClassLoader);
		read(in);
	}

	/**
	 * Creates a serializer using this configuration, that is capable of reading data
	 * written by the serializer described by this configuration.
	 *
	 * @return the restored serializer.
	 */
	@Override
	public final TypeSerializer<T> restoreSerializer() {
		if (serializer == null) {
			throw new IllegalStateException(
					"Trying to restore the prior serializer via TypeSerializerConfigSnapshot, " +
					"but the prior serializer has not been set.");
		}
		else if (serializer instanceof UnloadableDummyTypeSerializer) {
			Throwable originalError = ((UnloadableDummyTypeSerializer<?>) serializer).getOriginalError();

			throw new IllegalStateException(
					"Could not Java-deserialize TypeSerializer while restoring checkpoint metadata for serializer " +
					"snapshot '" + getClass().getName() + "'. " +
					"Please update to the TypeSerializerSnapshot interface that removes Java Serialization to avoid " +
					"this problem in the future.", originalError);
		} else {
			return this.serializer;
		}
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
			TypeSerializer<T> newSerializer) {

		// in prior versions, the compatibility check was in the serializer itself, so we
		// delegate this call to the serializer.
		final CompatibilityResult<T> compatibility = newSerializer.ensureCompatibility(this);

		return compatibility.isRequiresMigration() ?
				TypeSerializerSchemaCompatibility.incompatible() :
				TypeSerializerSchemaCompatibility.compatibleAsIs();
	}
}
