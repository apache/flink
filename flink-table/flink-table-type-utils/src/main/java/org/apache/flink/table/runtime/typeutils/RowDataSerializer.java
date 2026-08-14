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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.StateSchemaEvolvingSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.AbstractPagedInputView;
import org.apache.flink.core.memory.AbstractPagedOutputView;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/** Serializer for {@link RowData}. */
@Internal
public class RowDataSerializer extends AbstractRowDataSerializer<RowData>
        implements StateSchemaEvolvingSerializer<RowData> {
    private static final long serialVersionUID = 1L;

    private BinaryRowDataSerializer binarySerializer;
    private final LogicalType[] types;
    private final @Nullable String[] fieldNames;
    private final TypeSerializer[] fieldSerializers;
    private final RowData.FieldGetter[] fieldGetters;

    /** Whether the job configuration opted in to state schema evolution. */
    private final transient boolean schemaEvolutionAllowed;

    /**
     * Whether this serializer is the value serializer of a state whose backend migrates restored
     * values, and may therefore admit a backward-compatible schema change. This is the bit the
     * snapshot carries into a compatibility check.
     */
    private final transient boolean stateSchemaEvolutionEnabled;

    private transient BinaryRowData reuseRow;
    private transient BinaryRowWriter reuseWriter;

    public RowDataSerializer(RowType rowType) {
        this(
                rowType.getChildren().toArray(new LogicalType[0]),
                rowType.getChildren().stream()
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new),
                rowType.getFieldNames().toArray(new String[0]));
    }

    public RowDataSerializer(LogicalType... types) {
        this(
                types,
                Arrays.stream(types)
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new));
    }

    public RowDataSerializer(LogicalType[] types, TypeSerializer<?>[] fieldSerializers) {
        this(types, fieldSerializers, null);
    }

    private RowDataSerializer(
            LogicalType[] types,
            TypeSerializer<?>[] fieldSerializers,
            @Nullable String[] fieldNames) {
        this(types, fieldSerializers, fieldNames, false, false);
    }

    private RowDataSerializer(
            LogicalType[] types,
            TypeSerializer<?>[] fieldSerializers,
            @Nullable String[] fieldNames,
            boolean schemaEvolutionAllowed,
            boolean stateSchemaEvolutionEnabled) {
        this.types = types;
        this.fieldNames = fieldNames;
        this.fieldSerializers = fieldSerializers;
        this.schemaEvolutionAllowed = schemaEvolutionAllowed;
        this.stateSchemaEvolutionEnabled = stateSchemaEvolutionEnabled;
        this.binarySerializer = new BinaryRowDataSerializer(types.length);
        this.fieldGetters =
                IntStream.range(0, types.length)
                        .mapToObj(i -> RowData.createFieldGetter(types[i], i))
                        .toArray(RowData.FieldGetter[]::new);
    }

    @VisibleForTesting
    @Nullable
    String[] getFieldNames() {
        return fieldNames;
    }

    @VisibleForTesting
    TypeSerializer[] fieldSerializers() {
        return fieldSerializers;
    }

    @VisibleForTesting
    boolean isSchemaEvolutionAllowed() {
        return schemaEvolutionAllowed;
    }

    @VisibleForTesting
    boolean isStateSchemaEvolutionEnabled() {
        return stateSchemaEvolutionEnabled;
    }

    /** Returns a copy that records the job configuration having opted in to schema evolution. */
    RowDataSerializer withSchemaEvolutionAllowed() {
        return new RowDataSerializer(
                types, fieldSerializers, fieldNames, true, stateSchemaEvolutionEnabled);
    }

    @Override
    public TypeSerializer<RowData> withStateSchemaEvolution() {
        return schemaEvolutionAllowed ? enableStateSchemaEvolution() : this;
    }

    /**
     * Returns a copy that admits a backward-compatible schema change, recursing down the nested
     * {@code ROW} spine.
     *
     * <p>The opt-in is checked once, by the caller, and never again below: nested field serializers
     * are built by {@link InternalSerializers} inside this class's constructor, so they never carry
     * the opt-in flag. Re-checking it one level down would silently no-op and short-circuit every
     * nested {@code ROW} evolution to incompatible, because an unarmed nested snapshot rejects the
     * change and that rejection propagates to the whole row.
     *
     * <p>The descent stops at anything that is not a {@code ROW}: the migration remap does not
     * reach inside an ARRAY, MAP, MULTISET, RAW or structured value, so an evolved one is rejected
     * by the leaf type-equality check instead. A structured type's serializer is a {@code
     * RowDataSerializer} too, but its type root is not {@code ROW}, so the guard excludes it.
     */
    private RowDataSerializer enableStateSchemaEvolution() {
        TypeSerializer<?>[] evolvingFieldSerializers = new TypeSerializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            evolvingFieldSerializers[i] =
                    types[i].getTypeRoot() == LogicalTypeRoot.ROW
                                    && fieldSerializers[i] instanceof RowDataSerializer
                            ? ((RowDataSerializer) fieldSerializers[i]).enableStateSchemaEvolution()
                            : fieldSerializers[i];
        }
        return new RowDataSerializer(
                types, evolvingFieldSerializers, fieldNames, schemaEvolutionAllowed, true);
    }

    @Override
    public TypeSerializer<RowData> duplicate() {
        TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        // Both evolution flags travel with the copy: a state backend registers a duplicate of the
        // serializer, so a flag dropped here would never reach the snapshot.
        return new RowDataSerializer(
                types,
                duplicateFieldSerializers,
                fieldNames,
                schemaEvolutionAllowed,
                stateSchemaEvolutionEnabled);
    }

    @Override
    public RowData createInstance() {
        // default use binary row to deserializer
        return new BinaryRowData(types.length);
    }

    @Override
    public void serialize(RowData row, DataOutputView target) throws IOException {
        binarySerializer.serialize(toBinaryRow(row), target);
    }

    @Override
    public RowData deserialize(DataInputView source) throws IOException {
        return binarySerializer.deserialize(source);
    }

    @Override
    public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
        if (reuse instanceof BinaryRowData) {
            return binarySerializer.deserialize((BinaryRowData) reuse, source);
        } else {
            return binarySerializer.deserialize(source);
        }
    }

    @Override
    public RowData copy(RowData from) {
        if (from.getArity() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: " + from.getArity() + ", but serializer arity: " + types.length);
        }
        if (from instanceof BinaryRowData) {
            return ((BinaryRowData) from).copy();
        } else if (from instanceof NestedRowData) {
            return ((NestedRowData) from).copy();
        } else {
            return copyRowData(from, new GenericRowData(from.getArity()));
        }
    }

    @Override
    public RowData copy(RowData from, RowData reuse) {
        if (from.getArity() != types.length || reuse.getArity() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: "
                            + from.getArity()
                            + ", reuse Row arity: "
                            + reuse.getArity()
                            + ", but serializer arity: "
                            + types.length);
        }
        if (from instanceof BinaryRowData) {
            return reuse instanceof BinaryRowData
                    ? ((BinaryRowData) from).copy((BinaryRowData) reuse)
                    : ((BinaryRowData) from).copy();
        } else if (from instanceof NestedRowData) {
            return reuse instanceof NestedRowData
                    ? ((NestedRowData) from).copy(reuse)
                    : ((NestedRowData) from).copy();
        } else {
            return copyRowData(from, reuse);
        }
    }

    @SuppressWarnings("unchecked")
    private RowData copyRowData(RowData from, RowData reuse) {
        GenericRowData ret;
        if (reuse instanceof GenericRowData) {
            ret = (GenericRowData) reuse;
        } else {
            ret = new GenericRowData(from.getArity());
        }
        ret.setRowKind(from.getRowKind());
        for (int i = 0; i < from.getArity(); i++) {
            if (!from.isNullAt(i)) {
                ret.setField(i, fieldSerializers[i].copy((fieldGetters[i].getFieldOrNull(from))));
            } else {
                ret.setField(i, null);
            }
        }
        return ret;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        binarySerializer.copy(source, target);
    }

    @Override
    public int getArity() {
        return types.length;
    }

    /** Convert {@link RowData} into {@link BinaryRowData}. TODO modify it to code gen. */
    @Override
    public BinaryRowData toBinaryRow(RowData row) {
        return toBinaryRow(row, false);
    }

    public BinaryRowData toBinaryRow(RowData row, boolean requiresDeepCopy) {
        if (row instanceof BinaryRowData) {
            return (BinaryRowData) row;
        }
        if (reuseRow == null || requiresDeepCopy) {
            reuseRow = new BinaryRowData(types.length);
            reuseWriter = new BinaryRowWriter(reuseRow);
        }
        reuseWriter.reset();
        reuseWriter.writeRowKind(row.getRowKind());
        for (int i = 0; i < types.length; i++) {
            if (row.isNullAt(i)) {
                reuseWriter.setNullAt(i);
            } else {
                BinaryWriter.write(
                        reuseWriter,
                        i,
                        fieldGetters[i].getFieldOrNull(row),
                        types[i],
                        fieldSerializers[i]);
            }
        }
        reuseWriter.complete();
        return reuseRow;
    }

    @Override
    public int serializeToPages(RowData row, AbstractPagedOutputView target) throws IOException {
        return binarySerializer.serializeToPages(toBinaryRow(row), target);
    }

    @Override
    public RowData deserializeFromPages(AbstractPagedInputView source) throws IOException {
        throw new UnsupportedOperationException("Not support!");
    }

    @Override
    public RowData deserializeFromPages(RowData reuse, AbstractPagedInputView source)
            throws IOException {
        throw new UnsupportedOperationException("Not support!");
    }

    @Override
    public RowData mapFromPages(RowData reuse, AbstractPagedInputView source) throws IOException {
        if (reuse instanceof BinaryRowData) {
            return binarySerializer.mapFromPages((BinaryRowData) reuse, source);
        } else {
            throw new UnsupportedOperationException("Not support!");
        }
    }

    @Override
    public void skipRecordFromPages(AbstractPagedInputView source) throws IOException {
        binarySerializer.skipRecordFromPages(source);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowDataSerializer) {
            RowDataSerializer other = (RowDataSerializer) obj;
            return Arrays.equals(fieldSerializers, other.fieldSerializers);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldSerializers);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
        return new RowDataSerializerSnapshot(
                types, fieldSerializers, fieldNames, stateSchemaEvolutionEnabled);
    }

    /** {@link TypeSerializerSnapshot} for {@link BinaryRowDataSerializer}. */
    public static final class RowDataSerializerSnapshot implements TypeSerializerSnapshot<RowData> {
        private static final int CURRENT_VERSION = 4;

        private LogicalType[] types;
        private @Nullable String[] fieldNames;
        private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

        /**
         * Whether the serializer this snapshot was taken from admits a backward-compatible schema
         * change. It is deliberately not part of the snapshot format: a snapshot read back from
         * bytes describes stored state, not a running job's configuration, so it always resolves
         * with evolution off.
         */
        private boolean stateSchemaEvolutionEnabled;

        @SuppressWarnings("unused")
        public RowDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        RowDataSerializerSnapshot(
                LogicalType[] types,
                TypeSerializer[] serializers,
                @Nullable String[] fieldNames,
                boolean stateSchemaEvolutionEnabled) {
            this.types = types;
            this.fieldNames = fieldNames;
            this.stateSchemaEvolutionEnabled = stateSchemaEvolutionEnabled;
            this.nestedSerializersSnapshotDelegate =
                    new NestedSerializersSnapshotDelegate(serializers);
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(types.length);
            DataOutputViewStream stream = new DataOutputViewStream(out);
            for (LogicalType previousType : types) {
                InstantiationUtil.serializeObject(stream, previousType);
            }
            nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
            // A false flag means no field names (null); the count is not stored because it always
            // equals the number of fields.
            out.writeBoolean(fieldNames != null);
            if (fieldNames != null) {
                for (String fieldName : fieldNames) {
                    out.writeUTF(fieldName);
                }
            }
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            int length = in.readInt();
            DataInputViewStream stream = new DataInputViewStream(in);
            types = new LogicalType[length];
            for (int i = 0; i < length; i++) {
                try {
                    types[i] =
                            InstantiationUtil.deserializeObject(stream, userCodeClassLoader, true);
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }
            this.nestedSerializersSnapshotDelegate =
                    NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
                            in, userCodeClassLoader);
            if (readVersion >= 4 && in.readBoolean()) {
                fieldNames = new String[types.length];
                for (int i = 0; i < types.length; i++) {
                    fieldNames[i] = in.readUTF();
                }
            }
        }

        @Override
        public RowDataSerializer restoreSerializer() {
            return new RowDataSerializer(
                    types,
                    nestedSerializersSnapshotDelegate.getRestoredNestedSerializers(),
                    fieldNames);
        }

        @Override
        public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(
                TypeSerializerSnapshot<RowData> oldSerializerSnapshot) {
            if (!(oldSerializerSnapshot instanceof RowDataSerializerSnapshot)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            RowDataSerializerSnapshot oldRowDataSerializerSnapshot =
                    (RowDataSerializerSnapshot) oldSerializerSnapshot;
            // A side that carries no names cannot disagree with anything: without names a reorder
            // is undetectable, so identical types keep meaning "compatible as is" there, exactly as
            // they do unarmed.
            boolean namesDisagree =
                    stateSchemaEvolutionEnabled
                            && fieldNames != null
                            && oldRowDataSerializerSnapshot.fieldNames != null
                            && !Arrays.equals(fieldNames, oldRowDataSerializerSnapshot.fieldNames);

            // Identical positional layout: the nested composite path. Equal types at equal
            // positions say nothing about which field is which, so an armed resolution has to see
            // the names agree as well before it can treat the layout as unchanged -- otherwise a
            // reorder or a rename among same-typed fields resolves here as needing no migration
            // and leaves every value sitting under a neighbour's name.
            if (Arrays.equals(types, oldRowDataSerializerSnapshot.types) && !namesDisagree) {
                CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData>
                        intermediateResult =
                                CompositeTypeSerializerUtil
                                        .constructIntermediateCompatibilityResult(
                                                nestedSerializersSnapshotDelegate
                                                        .getNestedSerializerSnapshots(),
                                                oldRowDataSerializerSnapshot
                                                        .nestedSerializersSnapshotDelegate
                                                        .getNestedSerializerSnapshots());

                if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
                    RowDataSerializer reconfiguredCompositeSerializer = restoreSerializer();
                    return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                            reconfiguredCompositeSerializer);
                }

                return intermediateResult.getFinalResult();
            }

            if (!stateSchemaEvolutionEnabled) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            // The new side must carry field names. A name-less new serializer is reachable -- a
            // structured type resolves to one -- and admitting it would open a permanent name-less
            // evolution channel rather than a ramp for savepoints taken before names were stored.
            if (fieldNames == null) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            return oldRowDataSerializerSnapshot.fieldNames != null
                    ? checkNameBasedEvolution(oldRowDataSerializerSnapshot)
                    : checkPositionalEvolution(oldRowDataSerializerSnapshot);
        }

        private TypeSerializerSchemaCompatibility<RowData> checkNameBasedEvolution(
                RowDataSerializerSnapshot oldSnapshot) {
            int[] oldToNew = buildNameMapping(oldSnapshot.fieldNames, this.fieldNames);
            int[] newToOld = buildNameMapping(this.fieldNames, oldSnapshot.fieldNames);

            // (A) Every new-only field (no matching old field) must be nullable.
            for (int newPos = 0; newPos < newToOld.length; newPos++) {
                if (newToOld[newPos] == -1 && !types[newPos].isNullable()) {
                    return TypeSerializerSchemaCompatibility.incompatible();
                }
            }

            // (B) Every old field must survive with a compatible type, and nested snapshots are
            //     aligned old->new so nested ROW evolution can recurse. Leaf (non-ROW) fields
            //     require an exactly equal type; ROW fields defer to the nested recursion in (C).
            TypeSerializerSnapshot<?>[] newNested =
                    nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots();
            TypeSerializerSnapshot<?>[] alignedNewNested =
                    new TypeSerializerSnapshot<?>[oldSnapshot.types.length];
            for (int oldPos = 0; oldPos < oldToNew.length; oldPos++) {
                int newPos = oldToNew[oldPos];
                if (newPos == -1) {
                    return TypeSerializerSchemaCompatibility.incompatible(); // field removed
                }
                LogicalType oldType = oldSnapshot.types[oldPos];
                LogicalType newType = types[newPos];
                if (!bothRow(oldType, newType) && !oldType.equals(newType)) {
                    return TypeSerializerSchemaCompatibility.incompatible(); // leaf type changed
                }
                alignedNewNested[oldPos] = newNested[newPos];
            }

            // (C) Recurse into the aligned nested snapshot pairs.
            return resolveAlignedNested(alignedNewNested, oldSnapshot);
        }

        /**
         * Resolves against a prior snapshot that carries no field names, matching fields by
         * position.
         *
         * <p>Position is a stable identity only for an append. An insertion in the middle is
         * indistinguishable from a retype plus an append, and the two demand opposite migrations,
         * so the old layout has to be a prefix of the new one.
         */
        private TypeSerializerSchemaCompatibility<RowData> checkPositionalEvolution(
                RowDataSerializerSnapshot oldSnapshot) {
            if (types.length < oldSnapshot.types.length) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            for (int i = 0; i < oldSnapshot.types.length; i++) {
                LogicalType oldType = oldSnapshot.types[i];
                LogicalType newType = types[i];
                if (!bothRow(oldType, newType) && !oldType.equals(newType)) {
                    return TypeSerializerSchemaCompatibility.incompatible();
                }
            }
            for (int i = oldSnapshot.types.length; i < types.length; i++) {
                if (!types[i].isNullable()) {
                    return TypeSerializerSchemaCompatibility.incompatible();
                }
            }

            // constructIntermediateCompatibilityResult requires both arrays to have the same
            // length, so only the prefix the old layout covers is handed to it.
            TypeSerializerSnapshot<?>[] alignedNewNested =
                    Arrays.copyOf(
                            nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots(),
                            oldSnapshot.types.length);
            return resolveAlignedNested(alignedNewNested, oldSnapshot);
        }

        private TypeSerializerSchemaCompatibility<RowData> resolveAlignedNested(
                TypeSerializerSnapshot<?>[] alignedNewNested,
                RowDataSerializerSnapshot oldSnapshot) {
            CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData> nested =
                    CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                            alignedNewNested,
                            oldSnapshot.nestedSerializersSnapshotDelegate
                                    .getNestedSerializerSnapshots());
            // A reconfigured nested serializer is deliberately not propagated here, unlike on the
            // identical-layout path. Reconfiguration exists so a new serializer can read old bytes;
            // once the values have been remapped there are no old bytes left, because the migrated
            // row is re-encoded by the state's own new serializer.
            return nested.isIncompatible()
                    ? TypeSerializerSchemaCompatibility.incompatible()
                    : TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }

        private static boolean bothRow(LogicalType oldType, LogicalType newType) {
            return oldType.getTypeRoot() == LogicalTypeRoot.ROW
                    && newType.getTypeRoot() == LogicalTypeRoot.ROW;
        }

        @Override
        public RowData migrate(
                TypeSerializerSnapshot<RowData> oldSerializerSnapshot, RowData value) {
            if (value == null) {
                return null;
            }
            // Runs once per restored entry, so a mismatch would otherwise surface as a bare
            // ClassCastException from deep inside a migration loop.
            Preconditions.checkArgument(
                    oldSerializerSnapshot instanceof RowDataSerializerSnapshot,
                    "Cannot migrate RowData state from %s.",
                    oldSerializerSnapshot.getClass().getName());
            RowDataSerializerSnapshot oldSnapshot =
                    (RowDataSerializerSnapshot) oldSerializerSnapshot;
            return getNewRowData(value, oldSnapshot.restoreSerializer(), restoreSerializer());
        }

        // Remaps oldData into the new layout. Name-based when both serializers carry field names;
        // otherwise positions map 1:1 up to the common field count. RowKind preserved; added
        // fields and null sources become null; nested ROW values are remapped recursively.
        private static GenericRowData getNewRowData(
                RowData oldData, RowDataSerializer oldSerializer, RowDataSerializer newSerializer) {
            GenericRowData newData = new GenericRowData(newSerializer.getArity());
            newData.setRowKind(oldData.getRowKind());
            int[] positions = buildPositionMapping(oldSerializer, newSerializer);
            for (int newPos = 0; newPos < newSerializer.getArity(); newPos++) {
                int oldPos = positions[newPos];
                if (oldPos != -1 && !oldData.isNullAt(oldPos)) {
                    Object fieldValue = oldSerializer.fieldGetters[oldPos].getFieldOrNull(oldData);
                    if (fieldValue instanceof RowData) {
                        fieldValue =
                                getNewRowData(
                                        (RowData) fieldValue,
                                        (RowDataSerializer) oldSerializer.fieldSerializers[oldPos],
                                        (RowDataSerializer) newSerializer.fieldSerializers[newPos]);
                    }
                    newData.setField(newPos, fieldValue);
                } else {
                    newData.setField(newPos, null);
                }
            }
            return newData;
        }

        // positions[newPos] = matching old position, or -1 for an added field.
        private static int[] buildPositionMapping(
                RowDataSerializer oldSerializer, RowDataSerializer newSerializer) {
            if (oldSerializer.getFieldNames() != null && newSerializer.getFieldNames() != null) {
                // Already indexed by new position, one entry per new field.
                return buildNameMapping(
                        newSerializer.getFieldNames(), oldSerializer.getFieldNames());
            }
            // The old arity comes from the serializer, not from the record: the compatibility rule
            // is a statement about the snapshot's layout, and reading it off the record would make
            // the remap follow whatever turned up instead. Under the prefix bound the two agree,
            // which is exactly what would make a divergence invisible.
            int[] positions = new int[newSerializer.getArity()];
            int commonFields = Math.min(oldSerializer.getArity(), newSerializer.getArity());
            for (int i = 0; i < newSerializer.getArity(); i++) {
                positions[i] = i < commonFields ? i : -1;
            }
            return positions;
        }

        // mapping[i] = index in toNames of the field named fromNames[i], or -1 if absent.
        private static int[] buildNameMapping(String[] fromNames, String[] toNames) {
            Map<String, Integer> toIndex = new HashMap<>(toNames.length);
            for (int i = 0; i < toNames.length; i++) {
                toIndex.put(toNames[i], i);
            }
            int[] mapping = new int[fromNames.length];
            for (int i = 0; i < fromNames.length; i++) {
                mapping[i] = toIndex.getOrDefault(fromNames[i], -1);
            }
            return mapping;
        }
    }
}
