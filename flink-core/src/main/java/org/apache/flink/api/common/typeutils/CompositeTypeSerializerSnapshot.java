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
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.IntermediateCompatibilityResult;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link CompositeTypeSerializerSnapshot} is a convenient serializer snapshot class that can be
 * used by simple serializers which 1) delegates its serialization to multiple nested serializers,
 * and 2) may contain some extra static information that needs to be persisted as part of its
 * snapshot.
 *
 * <p>Examples for this would be the {@link ListSerializer}, {@link MapSerializer}, {@link
 * EitherSerializer}, etc., in which case the serializer, called the "outer" serializer in this
 * context, has only some nested serializers that needs to be persisted as its snapshot, and nothing
 * else that needs to be persisted as the "outer" snapshot. An example which has non-empty outer
 * snapshots would be the {@link GenericArraySerializer}, which beyond the nested component
 * serializer, also contains a class of the component type that needs to be persisted.
 *
 * <p>Serializers that do have some outer snapshot needs to make sure to implement the methods
 * {@link #writeOuterSnapshot(DataOutputView)}, {@link #readOuterSnapshot(int, DataInputView,
 * ClassLoader)}, and {@link #resolveOuterSchemaCompatibility(TypeSerializer)} when using this class
 * as the base for its serializer snapshot class. By default, the base implementations of these
 * methods are empty, i.e. this class assumes that subclasses do not have any outer snapshot that
 * needs to be persisted.
 *
 * <h2>Snapshot Versioning</h2>
 *
 * <p>This base class has its own versioning for the format in which it writes the outer snapshot
 * and the nested serializer snapshots. The version of the serialization format of this based class
 * is defined by {@link #getCurrentVersion()}. This is independent of the version in which
 * subclasses writes their outer snapshot, defined by {@link #getCurrentOuterSnapshotVersion()}.
 * This means that the outer snapshot's version can be maintained only taking into account changes
 * in how the outer snapshot is written. Any changes in the base format does not require upticks in
 * the outer snapshot's version.
 *
 * <h2>Serialization Format</h2>
 *
 * <p>The current version of the serialization format of a {@link CompositeTypeSerializerSnapshot}
 * is as follows:
 *
 * <pre>{@code
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | CompositeTypeSerializerSnapshot | CompositeTypeSerializerSnapshot |          Outer snapshot         |
 * |           version               |          MAGIC_NUMBER           |              version            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                               Outer snapshot                                        |
 * |                                   #writeOuterSnapshot(DataOutputView out)                           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |      Delegate MAGIC_NUMBER      |         Delegate version        |     Num. nested serializers     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                     Nested serializer snapshots                                     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }</pre>
 *
 * @param <T> The data type that the originating serializer of this snapshot serializes.
 * @param <S> The type of the originating serializer.
 */
@PublicEvolving
public abstract class CompositeTypeSerializerSnapshot<T, S extends TypeSerializer<T>>
        implements TypeSerializerSnapshot<T> {

    /**
     * Indicates schema compatibility of the serializer configuration persisted as the outer
     * snapshot.
     */
    protected enum OuterSchemaCompatibility {
        COMPATIBLE_AS_IS,
        COMPATIBLE_AFTER_MIGRATION,
        INCOMPATIBLE
    }

    /** Magic number for integrity checks during deserialization. */
    private static final int MAGIC_NUMBER = 911108;

    /**
     * Current version of the base serialization format.
     *
     * <p>NOTE: We start from version 3. This version is represented by the {@link
     * #getCurrentVersion()} method. Previously, this method was used to represent the outer
     * snapshot's version (now, represented by the {@link #getCurrentOuterSnapshotVersion()}
     * method).
     *
     * <p>To bridge this transition, we set the starting version of the base format to be at least
     * larger than the highest version of previously defined values in implementing subclasses,
     * which was {@link #HIGHEST_LEGACY_READ_VERSION}. This allows us to identify legacy
     * deserialization paths, which did not contain versioning for the base format, simply by
     * checking if the read version of the snapshot is smaller than or equal to {@link
     * #HIGHEST_LEGACY_READ_VERSION}.
     */
    private static final int VERSION = 3;

    private static final int HIGHEST_LEGACY_READ_VERSION = 2;

    private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

    private final Class<S> correspondingSerializerClass;

    /**
     * Constructor to be used for read instantiation.
     *
     * @param correspondingSerializerClass the expected class of the new serializer.
     */
    @SuppressWarnings("unchecked")
    public CompositeTypeSerializerSnapshot(
            Class<? extends TypeSerializer> correspondingSerializerClass) {
        this.correspondingSerializerClass = (Class<S>) checkNotNull(correspondingSerializerClass);
    }

    /**
     * Constructor to be used for writing the snapshot.
     *
     * @param serializerInstance an instance of the originating serializer of this snapshot.
     */
    @SuppressWarnings("unchecked")
    public CompositeTypeSerializerSnapshot(S serializerInstance) {
        checkNotNull(serializerInstance);
        this.nestedSerializersSnapshotDelegate =
                new NestedSerializersSnapshotDelegate(getNestedSerializers(serializerInstance));
        this.correspondingSerializerClass = (Class<S>) serializerInstance.getClass();
    }

    @Override
    public final int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public final void writeSnapshot(DataOutputView out) throws IOException {
        internalWriteOuterSnapshot(out);
        nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
    }

    @Override
    public final void readSnapshot(
            int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        if (readVersion > HIGHEST_LEGACY_READ_VERSION) {
            internalReadOuterSnapshot(in, userCodeClassLoader);
        } else {
            legacyInternalReadOuterSnapshot(readVersion, in, userCodeClassLoader);
        }
        this.nestedSerializersSnapshotDelegate =
                NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
                        in, userCodeClassLoader);
    }

    public TypeSerializerSnapshot<?>[] getNestedSerializerSnapshots() {
        return nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots();
    }

    @Override
    public final TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {
        return internalResolveSchemaCompatibility(
                newSerializer, nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots());
    }

    @Internal
    TypeSerializerSchemaCompatibility<T> internalResolveSchemaCompatibility(
            TypeSerializer<T> newSerializer, TypeSerializerSnapshot<?>[] snapshots) {
        if (newSerializer.getClass() != correspondingSerializerClass) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        S castedNewSerializer = correspondingSerializerClass.cast(newSerializer);

        final OuterSchemaCompatibility outerSchemaCompatibility =
                resolveOuterSchemaCompatibility(castedNewSerializer);

        final TypeSerializer<?>[] newNestedSerializers = getNestedSerializers(castedNewSerializer);
        // check that nested serializer arity remains identical; if not, short circuit result
        if (newNestedSerializers.length != snapshots.length) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        return constructFinalSchemaCompatibilityResult(
                newNestedSerializers, snapshots, outerSchemaCompatibility);
    }

    @Internal
    void setNestedSerializersSnapshotDelegate(NestedSerializersSnapshotDelegate delegate) {
        this.nestedSerializersSnapshotDelegate = checkNotNull(delegate);
    }

    @Override
    public final TypeSerializer<T> restoreSerializer() {
        @SuppressWarnings("unchecked")
        TypeSerializer<T> serializer =
                (TypeSerializer<T>)
                        createOuterSerializerWithNestedSerializers(
                                nestedSerializersSnapshotDelegate.getRestoredNestedSerializers());

        return serializer;
    }

    // ------------------------------------------------------------------------------------------
    //  Outer serializer access methods
    // ------------------------------------------------------------------------------------------

    /**
     * Returns the version of the current outer snapshot's written binary format.
     *
     * @return the version of the current outer snapshot's written binary format.
     */
    protected abstract int getCurrentOuterSnapshotVersion();

    /**
     * Gets the nested serializers from the outer serializer.
     *
     * @param outerSerializer the outer serializer.
     * @return the nested serializers.
     */
    protected abstract TypeSerializer<?>[] getNestedSerializers(S outerSerializer);

    /**
     * Creates an instance of the outer serializer with a given array of its nested serializers.
     *
     * @param nestedSerializers array of nested serializers to create the outer serializer with.
     * @return an instance of the outer serializer.
     */
    protected abstract S createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers);

    // ------------------------------------------------------------------------------------------
    //  Outer snapshot methods; need to be overridden if outer snapshot is not empty,
    //  or in other words, the outer serializer has extra configuration beyond its nested
    // serializers.
    // ------------------------------------------------------------------------------------------

    /**
     * Writes the outer snapshot, i.e. any information beyond the nested serializers of the outer
     * serializer.
     *
     * <p>The base implementation of this methods writes nothing, i.e. it assumes that the outer
     * serializer only has nested serializers and no extra information. Otherwise, if the outer
     * serializer contains some extra information that needs to be persisted as part of the
     * serializer snapshot, this must be overridden. Note that this method and the corresponding
     * methods {@link #readOuterSnapshot(int, DataInputView, ClassLoader)}, {@link
     * #resolveOuterSchemaCompatibility(TypeSerializer)} needs to be implemented.
     *
     * @param out the {@link DataOutputView} to write the outer snapshot to.
     */
    protected void writeOuterSnapshot(DataOutputView out) throws IOException {}

    /**
     * Reads the outer snapshot, i.e. any information beyond the nested serializers of the outer
     * serializer.
     *
     * <p>The base implementation of this methods reads nothing, i.e. it assumes that the outer
     * serializer only has nested serializers and no extra information. Otherwise, if the outer
     * serializer contains some extra information that has been persisted as part of the serializer
     * snapshot, this must be overridden. Note that this method and the corresponding methods {@link
     * #writeOuterSnapshot(DataOutputView)}, {@link
     * #resolveOuterSchemaCompatibility(TypeSerializer)} needs to be implemented.
     *
     * @param readOuterSnapshotVersion the read version of the outer snapshot.
     * @param in the {@link DataInputView} to read the outer snapshot from.
     * @param userCodeClassLoader the user code class loader.
     */
    protected void readOuterSnapshot(
            int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {}

    /**
     * Checks whether the outer snapshot is compatible with a given new serializer.
     *
     * <p>The base implementation of this method just returns {@code true}, i.e. it assumes that the
     * outer serializer only has nested serializers and no extra information, and therefore the
     * result of the check must always be true. Otherwise, if the outer serializer contains some
     * extra information that has been persisted as part of the serializer snapshot, this must be
     * overridden. Note that this method and the corresponding methods {@link
     * #writeOuterSnapshot(DataOutputView)}, {@link #readOuterSnapshot(int, DataInputView,
     * ClassLoader)} needs to be implemented.
     *
     * @param newSerializer the new serializer, which contains the new outer information to check
     *     against.
     * @return a flag indicating whether or not the new serializer's outer information is compatible
     *     with the one written in this snapshot.
     * @deprecated this method is deprecated, and will be removed in the future. Please implement
     *     {@link #resolveOuterSchemaCompatibility(TypeSerializer)} instead.
     */
    @Deprecated
    protected boolean isOuterSnapshotCompatible(S newSerializer) {
        return true;
    }

    /**
     * Checks the schema compatibility of the given new serializer based on the outer snapshot.
     *
     * <p>The base implementation of this method assumes that the outer serializer only has nested
     * serializers and no extra information, and therefore the result of the check is {@link
     * OuterSchemaCompatibility#COMPATIBLE_AS_IS}. Otherwise, if the outer serializer contains some
     * extra information that has been persisted as part of the serializer snapshot, this must be
     * overridden. Note that this method and the corresponding methods {@link
     * #writeOuterSnapshot(DataOutputView)}, {@link #readOuterSnapshot(int, DataInputView,
     * ClassLoader)} needs to be implemented.
     *
     * @param newSerializer the new serializer, which contains the new outer information to check
     *     against.
     * @return a {@link OuterSchemaCompatibility} indicating whether or the new serializer's outer
     *     information is compatible, requires migration, or incompatible with the one written in
     *     this snapshot.
     */
    protected OuterSchemaCompatibility resolveOuterSchemaCompatibility(S newSerializer) {
        return (isOuterSnapshotCompatible(newSerializer))
                ? OuterSchemaCompatibility.COMPATIBLE_AS_IS
                : OuterSchemaCompatibility.INCOMPATIBLE;
    }

    // ------------------------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------------------------

    private void internalWriteOuterSnapshot(DataOutputView out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(getCurrentOuterSnapshotVersion());

        writeOuterSnapshot(out);
    }

    private void internalReadOuterSnapshot(DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format(
                            "Corrupt data, magic number mismatch. Expected %8x, found %8x",
                            MAGIC_NUMBER, magicNumber));
        }

        final int outerSnapshotVersion = in.readInt();
        readOuterSnapshot(outerSnapshotVersion, in, userCodeClassLoader);
    }

    private void legacyInternalReadOuterSnapshot(
            int legacyReadVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {

        // legacy versions did not contain the pre-fixed magic numbers; just read the outer snapshot
        readOuterSnapshot(legacyReadVersion, in, userCodeClassLoader);
    }

    private TypeSerializerSchemaCompatibility<T> constructFinalSchemaCompatibilityResult(
            TypeSerializer<?>[] newNestedSerializers,
            TypeSerializerSnapshot<?>[] nestedSerializerSnapshots,
            OuterSchemaCompatibility outerSchemaCompatibility) {

        IntermediateCompatibilityResult<T> nestedSerializersCompatibilityResult =
                CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                        newNestedSerializers, nestedSerializerSnapshots);

        if (outerSchemaCompatibility == OuterSchemaCompatibility.INCOMPATIBLE
                || nestedSerializersCompatibilityResult.isIncompatible()) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (outerSchemaCompatibility == OuterSchemaCompatibility.COMPATIBLE_AFTER_MIGRATION
                || nestedSerializersCompatibilityResult.isCompatibleAfterMigration()) {
            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }

        if (nestedSerializersCompatibilityResult.isCompatibleWithReconfiguredSerializer()) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> reconfiguredCompositeSerializer =
                    createOuterSerializerWithNestedSerializers(
                            nestedSerializersCompatibilityResult.getNestedSerializers());
            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    reconfiguredCompositeSerializer);
        }

        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
}
