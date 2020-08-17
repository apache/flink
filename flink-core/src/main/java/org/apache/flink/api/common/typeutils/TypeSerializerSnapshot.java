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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot.ADAPTER_VERSION;

/**
 * A {@code TypeSerializerSnapshot} is a point-in-time view of a {@link TypeSerializer}'s configuration.
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
 *   This is performed by providing the new serializer to the corresponding serializer configuration
 *   snapshots in checkpoints.</li>
 *
 *   <li><strong>Factory for a read serializer when schema conversion is required:</strong> in the case that new
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
public interface TypeSerializerSnapshot<T> {

	/**
	 * Returns the version of the current snapshot's written binary format.
	 *
	 * @return the version of the current snapshot's written binary format.
	 */
	int getCurrentVersion();

	/**
	 * Writes the serializer snapshot to the provided {@link DataOutputView}.
	 * The current version of the written serializer snapshot's binary format
	 * is specified by the {@link #getCurrentVersion()} method.
	 *
	 * @param out the {@link DataOutputView} to write the snapshot to.
	 *
	 * @throws IOException Thrown if the snapshot data could not be written.
	 *
	 * @see #writeVersionedSnapshot(DataOutputView, TypeSerializerSnapshot)
	 */
	void writeSnapshot(DataOutputView out) throws IOException;

	/**
	 * Reads the serializer snapshot from the provided {@link DataInputView}.
	 * The version of the binary format that the serializer snapshot was written
	 * with is provided. This version can be used to determine how the serializer
	 * snapshot should be read.
	 *
	 * @param readVersion version of the serializer snapshot's written binary format
	 * @param in the {@link DataInputView} to read the snapshot from.
	 * @param userCodeClassLoader the user code classloader
	 *
	 * @throws IOException Thrown if the snapshot data could be read or parsed.
	 *
	 * @see #readVersionedSnapshot(DataInputView, ClassLoader)
	 */
	void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException;

	/**
	 * Recreates a serializer instance from this snapshot. The returned
	 * serializer can be safely used to read data written by the prior serializer
	 * (i.e., the serializer that created this snapshot).
	 *
	 * @return a serializer instance restored from this serializer snapshot.
	 */
	TypeSerializer<T> restoreSerializer();

	/**
	 * Checks a new serializer's compatibility to read data written by the prior serializer.
	 *
	 * <p>When a checkpoint/savepoint is restored, this method checks whether the serialization
	 * format of the data in the checkpoint/savepoint is compatible for the format of the serializer used by the
	 * program that restores the checkpoint/savepoint. The outcome can be that the serialization format is
	 * compatible, that the program's serializer needs to reconfigure itself (meaning to incorporate some
	 * information from the TypeSerializerSnapshot to be compatible), that the format is outright incompatible,
	 * or that a migration needed. In the latter case, the TypeSerializerSnapshot produces a serializer to
	 * deserialize the data, and the restoring program's serializer re-serializes the data, thus converting
	 * the format during the restore operation.
	 *
	 * @param newSerializer the new serializer to check.
	 *
	 * @return the serializer compatibility result.
	 */
	TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer);

	// ------------------------------------------------------------------------
	//  read / write utilities
	// ------------------------------------------------------------------------

	/**
	 * Writes the given snapshot to the out stream. One should always use this method to write
	 * snapshots out, rather than directly calling {@link #writeSnapshot(DataOutputView)}.
	 *
	 * <p>The snapshot written with this method can be read via {@link #readVersionedSnapshot(DataInputView, ClassLoader)}.
	 */
	static void writeVersionedSnapshot(DataOutputView out, TypeSerializerSnapshot<?> snapshot) throws IOException {
		out.writeUTF(snapshot.getClass().getName());
		out.writeInt(snapshot.getCurrentVersion());
		snapshot.writeSnapshot(out);
	}

	/**
	 * Reads a snapshot from the stream, performing resolving
	 *
	 * <p>This method reads snapshots written by {@link #writeVersionedSnapshot(DataOutputView, TypeSerializerSnapshot)}.
	 */
	static <T> TypeSerializerSnapshot<T> readVersionedSnapshot(DataInputView in, ClassLoader cl) throws IOException {
		final TypeSerializerSnapshot<T> snapshot =
				TypeSerializerSnapshotSerializationUtil.readAndInstantiateSnapshotClass(in, cl);

		int version = in.readInt();

		if (version == ADAPTER_VERSION && !(snapshot instanceof TypeSerializerConfigSnapshot)) {
			// the snapshot was upgraded directly in-place from a TypeSerializerConfigSnapshot;
			// read and drop the previously Java-serialized serializer, and get the actual correct read version.
			// NOTE: this implicitly assumes that the version was properly written before the actual snapshot content.
			TypeSerializerSerializationUtil.tryReadSerializer(in, cl, true);
			version = in.readInt();
		}
		snapshot.readSnapshot(version, in, cl);

		return snapshot;
	}
}
