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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** Serializer for {@link RowData}. */
@Internal
public class RowDataSerializer extends AbstractRowDataSerializer<RowData> {
    private static final long serialVersionUID = 1L;

    private BinaryRowDataSerializer binarySerializer;
    private final LogicalType[] types;
    private final @Nullable String[] fieldNames;
    private final TypeSerializer[] fieldSerializers;
    private final RowData.FieldGetter[] fieldGetters;

    private transient BinaryRowData reuseRow;
    private transient BinaryRowWriter reuseWriter;

    public RowDataSerializer(RowType rowType) {
        this(
                rowType.getChildren().toArray(new LogicalType[0]),
                rowType.getChildren().stream()
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new));
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
        this.types = types;
        this.fieldNames = fieldNames;
        this.fieldSerializers = fieldSerializers;
        this.binarySerializer = new BinaryRowDataSerializer(types.length);
        this.fieldGetters =
                IntStream.range(0, types.length)
                        .mapToObj(i -> RowData.createFieldGetter(types[i], i))
                        .toArray(RowData.FieldGetter[]::new);
    }

    /**
     * Creates a serializer that retains the field names of {@code rowType} so that state written by
     * this serializer can be migrated by matching fields on name. Nested {@code ROW} children are
     * built recursively so their field names are retained at every level; all other children use
     * the default name-less serializers. Used for state-value serializers when schema evolution is
     * enabled; the name-less constructors remain the default (e.g. for state keys).
     */
    public static RowDataSerializer withFieldNames(RowType rowType) {
        List<LogicalType> children = rowType.getChildren();
        LogicalType[] types = children.toArray(new LogicalType[0]);
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer[children.size()];
        for (int i = 0; i < children.size(); i++) {
            LogicalType child = children.get(i);
            fieldSerializers[i] =
                    child.getTypeRoot() == LogicalTypeRoot.ROW
                            ? withFieldNames((RowType) child)
                            : InternalSerializers.create(child);
        }
        return new RowDataSerializer(
                types, fieldSerializers, rowType.getFieldNames().toArray(new String[0]));
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

    @Override
    public TypeSerializer<RowData> duplicate() {
        TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        // Field names must be carried through duplication: state backends duplicate the registered
        // serializer, and name-based schema-evolution compatibility relies on them being present.
        return new RowDataSerializer(types, duplicateFieldSerializers, fieldNames);
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
        return new RowDataSerializerSnapshot(types, fieldSerializers, fieldNames);
    }

    /** {@link TypeSerializerSnapshot} for {@link BinaryRowDataSerializer}. */
    public static final class RowDataSerializerSnapshot implements TypeSerializerSnapshot<RowData> {
        private static final int CURRENT_VERSION = 4;

        private LogicalType[] types;
        private @Nullable String[] fieldNames;
        private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

        @SuppressWarnings("unused")
        public RowDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        RowDataSerializerSnapshot(
                LogicalType[] types, TypeSerializer[] serializers, @Nullable String[] fieldNames) {
            this.types = types;
            this.fieldNames = fieldNames;
            this.nestedSerializersSnapshotDelegate =
                    new NestedSerializersSnapshotDelegate(serializers);
        }

        @VisibleForTesting
        @Nullable
        String[] getFieldNames() {
            return fieldNames;
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
            boolean hasFieldNames = fieldNames != null;
            out.writeBoolean(hasFieldNames);
            if (hasFieldNames) {
                for (String fieldName : fieldNames) {
                    out.writeUTF(fieldName);
                }
            }
            nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
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
            if (readVersion >= 4) {
                boolean hasFieldNames = in.readBoolean();
                if (hasFieldNames) {
                    fieldNames = new String[length];
                    for (int i = 0; i < length; i++) {
                        fieldNames[i] = in.readUTF();
                    }
                }
            }
            this.nestedSerializersSnapshotDelegate =
                    NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
                            in, userCodeClassLoader);
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
            RowDataSerializerSnapshot oldSnapshot =
                    (RowDataSerializerSnapshot) oldSerializerSnapshot;

            // Identical layout: preserve today's nested composite behavior exactly (no regression).
            if (Arrays.equals(types, oldSnapshot.types)) {
                CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData>
                        intermediateResult =
                                CompositeTypeSerializerUtil
                                        .constructIntermediateCompatibilityResult(
                                                nestedSerializersSnapshotDelegate
                                                        .getNestedSerializerSnapshots(),
                                                oldSnapshot.nestedSerializersSnapshotDelegate
                                                        .getNestedSerializerSnapshots());
                if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
                    return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                            restoreSerializer());
                }
                return intermediateResult.getFinalResult();
            }

            // Differing layout: name-based evolution requires field names on BOTH sides.
            if (fieldNames != null && oldSnapshot.fieldNames != null) {
                return checkNameBasedEvolution(oldSnapshot);
            }

            // Names absent on a side: preserve today's behavior (no positional relaxation).
            return TypeSerializerSchemaCompatibility.incompatible();
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
                boolean bothRow =
                        oldType.getTypeRoot() == LogicalTypeRoot.ROW
                                && newType.getTypeRoot() == LogicalTypeRoot.ROW;
                if (!bothRow && !oldType.equals(newType)) {
                    return TypeSerializerSchemaCompatibility.incompatible(); // leaf type changed
                }
                alignedNewNested[oldPos] = newNested[newPos];
            }

            // (C) Recurse: nested snapshot pairs run their own resolveSchemaCompatibility
            //     (nested ROW evolution is validated here).
            CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData> nested =
                    CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                            alignedNewNested,
                            oldSnapshot.nestedSerializersSnapshotDelegate
                                    .getNestedSerializerSnapshots());
            if (nested.isIncompatible()) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }

        @Override
        public RowData migrate(
                TypeSerializerSnapshot<RowData> oldSerializerSnapshot, RowData value) {
            RowDataSerializerSnapshot oldSnapshot =
                    (RowDataSerializerSnapshot) oldSerializerSnapshot;
            RowDataSerializer oldSerializer = oldSnapshot.restoreSerializer();
            RowDataSerializer newSerializer = restoreSerializer();
            return getNewRowData(value, oldSerializer, newSerializer);
        }

        // Remaps oldData into the new layout. Name-based when both serializers carry field names;
        // otherwise positions map 1:1 up to the common field count. RowKind preserved; added
        // fields and null sources become null; nested ROW values are remapped recursively.
        private static GenericRowData getNewRowData(
                RowData oldData, RowDataSerializer oldSerializer, RowDataSerializer newSerializer) {
            GenericRowData newData = new GenericRowData(newSerializer.getArity());
            newData.setRowKind(oldData.getRowKind());
            int[] positions = buildPositionMapping(oldData, oldSerializer, newSerializer);
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
                RowData oldData, RowDataSerializer oldSerializer, RowDataSerializer newSerializer) {
            int[] positions = new int[newSerializer.getArity()];
            if (oldSerializer.getFieldNames() != null && newSerializer.getFieldNames() != null) {
                int[] newToOld =
                        buildNameMapping(
                                newSerializer.getFieldNames(), oldSerializer.getFieldNames());
                System.arraycopy(newToOld, 0, positions, 0, newToOld.length);
            } else {
                int commonFields = Math.min(oldData.getArity(), newSerializer.getArity());
                for (int i = 0; i < newSerializer.getArity(); i++) {
                    positions[i] = i < commonFields ? i : -1;
                }
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
