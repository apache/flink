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

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A NestedSerializersSnapshotDelegate represents the snapshots of multiple serializers that are
 * used by an outer serializer. Examples would be tuples, where the outer serializer is the tuple
 * format serializer, and the NestedSerializersSnapshotDelegate holds the serializers for the
 * different tuple fields.
 *
 * <p>The NestedSerializersSnapshotDelegate does not implement the {@link TypeSerializerSnapshot}
 * interface. It is not meant to be inherited from, but to be composed with a serializer snapshot
 * implementation.
 *
 * <p>The NestedSerializersSnapshotDelegate has its own versioning internally, it does not couple
 * its versioning to the versioning of the TypeSerializerSnapshot that builds on top of this class.
 * That way, the NestedSerializersSnapshotDelegate and enclosing TypeSerializerSnapshot the can
 * evolve their formats independently.
 */
@Internal
public class NestedSerializersSnapshotDelegate {

    /** Magic number for integrity checks during deserialization. */
    private static final int MAGIC_NUMBER = 1333245;

    /** Current version of the new serialization format. */
    private static final int VERSION = 1;

    /** The snapshots from the serializer that make up this composition. */
    private final TypeSerializerSnapshot<?>[] nestedSnapshots;

    /** Constructor to create a snapshot for writing. */
    public NestedSerializersSnapshotDelegate(TypeSerializer<?>... serializers) {
        this.nestedSnapshots = TypeSerializerUtils.snapshot(serializers);
    }

    /** Constructor to create a snapshot during deserialization. */
    @Internal
    NestedSerializersSnapshotDelegate(TypeSerializerSnapshot<?>[] snapshots) {
        this.nestedSnapshots = checkNotNull(snapshots);
    }

    // ------------------------------------------------------------------------
    //  Nested Serializers and Compatibility
    // ------------------------------------------------------------------------

    /**
     * Produces a restore serializer from each contained serializer configuration snapshot. The
     * serializers are returned in the same order as the snapshots are stored.
     */
    public TypeSerializer<?>[] getRestoredNestedSerializers() {
        return snapshotsToRestoreSerializers(nestedSnapshots);
    }

    /** Creates the restore serializer from the pos-th config snapshot. */
    public <T> TypeSerializer<T> getRestoredNestedSerializer(int pos) {
        checkArgument(pos < nestedSnapshots.length);

        @SuppressWarnings("unchecked")
        TypeSerializerSnapshot<T> snapshot = (TypeSerializerSnapshot<T>) nestedSnapshots[pos];

        return snapshot.restoreSerializer();
    }

    /**
     * Returns the snapshots of the nested serializers.
     *
     * @return the snapshots of the nested serializers.
     */
    public TypeSerializerSnapshot<?>[] getNestedSerializerSnapshots() {
        return nestedSnapshots;
    }

    /**
     * Resolves the compatibility of the nested serializer snapshots with the nested serializers of
     * the new outer serializer.
     *
     * @deprecated this no method will be removed in the future. Resolving compatibility for nested
     *     serializers is now handled by {@link CompositeTypeSerializerSnapshot}.
     */
    @Deprecated
    public <T> TypeSerializerSchemaCompatibility<T> resolveCompatibilityWithNested(
            TypeSerializerSchemaCompatibility<?> outerCompatibility,
            TypeSerializer<?>... newNestedSerializers) {

        checkArgument(
                newNestedSerializers.length == nestedSnapshots.length,
                "Different number of new serializers and existing serializer configuration snapshots");

        // compatibility of the outer serializer's format
        if (outerCompatibility.isIncompatible()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        // check nested serializers for compatibility
        boolean nestedSerializerRequiresMigration = false;
        for (int i = 0; i < nestedSnapshots.length; i++) {
            TypeSerializerSchemaCompatibility<?> compatibility =
                    resolveCompatibility(newNestedSerializers[i], nestedSnapshots[i]);

            if (compatibility.isIncompatible()) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            if (compatibility.isCompatibleAfterMigration()) {
                nestedSerializerRequiresMigration = true;
            }
        }

        return (nestedSerializerRequiresMigration || !outerCompatibility.isCompatibleAsIs())
                ? TypeSerializerSchemaCompatibility.compatibleAfterMigration()
                : TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    // ------------------------------------------------------------------------
    //  Serialization
    // ------------------------------------------------------------------------

    /** Writes the composite snapshot of all the contained serializers. */
    public final void writeNestedSerializerSnapshots(DataOutputView out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(VERSION);

        out.writeInt(nestedSnapshots.length);
        for (TypeSerializerSnapshot<?> snap : nestedSnapshots) {
            TypeSerializerSnapshot.writeVersionedSnapshot(out, snap);
        }
    }

    /** Reads the composite snapshot of all the contained serializers. */
    public static NestedSerializersSnapshotDelegate readNestedSerializerSnapshots(
            DataInputView in, ClassLoader cl) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format(
                            "Corrupt data, magic number mismatch. Expected %8x, found %8x",
                            MAGIC_NUMBER, magicNumber));
        }

        final int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }

        final int numSnapshots = in.readInt();
        final TypeSerializerSnapshot<?>[] nestedSnapshots =
                new TypeSerializerSnapshot<?>[numSnapshots];

        for (int i = 0; i < numSnapshots; i++) {
            nestedSnapshots[i] = TypeSerializerSnapshot.readVersionedSnapshot(in, cl);
        }

        return new NestedSerializersSnapshotDelegate(nestedSnapshots);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /** Utility method to conjure up a new scope for the generic parameters. */
    @SuppressWarnings("unchecked")
    private static <E> TypeSerializerSchemaCompatibility<E> resolveCompatibility(
            TypeSerializer<?> serializer, TypeSerializerSnapshot<?> snapshot) {

        TypeSerializer<E> typedSerializer = (TypeSerializer<E>) serializer;
        TypeSerializerSnapshot<E> typedSnapshot = (TypeSerializerSnapshot<E>) snapshot;

        return typedSnapshot.resolveSchemaCompatibility(typedSerializer);
    }

    private static TypeSerializer<?>[] snapshotsToRestoreSerializers(
            TypeSerializerSnapshot<?>... snapshots) {
        return Arrays.stream(snapshots)
                .map(TypeSerializerSnapshot::restoreSerializer)
                .toArray(TypeSerializer[]::new);
    }
}
