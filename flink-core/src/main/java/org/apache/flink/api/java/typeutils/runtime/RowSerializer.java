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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.api.java.typeutils.runtime.MaskUtils.readIntoAndCopyMask;
import static org.apache.flink.api.java.typeutils.runtime.MaskUtils.readIntoMask;
import static org.apache.flink.api.java.typeutils.runtime.MaskUtils.writeMask;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serializer for {@link Row}.
 *
 * <p>It uses the following serialization format:
 *
 * <pre>
 *     |bitmask|field|field|....
 * </pre>
 *
 * The bitmask serves as a header that consists of {@link #ROW_KIND_OFFSET} bits for encoding the
 * {@link RowKind} and n bits for whether a field is null. For backwards compatibility, those bits
 * can be ignored if serializer runs in legacy mode:
 *
 * <pre>
 *     bitmask with row kind:  |RK RK F1 F2 ... FN|
 *     bitmask in legacy mode: |F1 F2 ... FN|
 * </pre>
 *
 * <p>Field names are an optional part of this serializer. They allow to use rows in named-based
 * field mode. However, the support for name-based rows is limited. Usually, name-based mode should
 * not be used in state but only for in-flight data. For now, names are not part of serializer
 * snapshot or equals/hashCode (similar to {@link RowTypeInfo}).
 */
@Internal
public final class RowSerializer extends TypeSerializer<Row> {

    public static final int ROW_KIND_OFFSET = 2;

    // legacy, don't touch until we drop support for 1.9 savepoints
    private static final long serialVersionUID = 1L;

    private final boolean supportsRowKind;

    private final int rowKindOffset;

    private final TypeSerializer<Object>[] fieldSerializers;

    private final int arity;

    private final @Nullable LinkedHashMap<String, Integer> positionByName;

    private transient boolean[] mask;

    private transient Row reuseRowPositionBased;

    public RowSerializer(TypeSerializer<?>[] fieldSerializers) {
        this(fieldSerializers, null, true);
    }

    public RowSerializer(
            TypeSerializer<?>[] fieldSerializers,
            @Nullable LinkedHashMap<String, Integer> positionByName) {
        this(fieldSerializers, positionByName, true);
    }

    @SuppressWarnings("unchecked")
    public RowSerializer(
            TypeSerializer<?>[] fieldSerializers,
            @Nullable LinkedHashMap<String, Integer> positionByName,
            boolean supportsRowKind) {
        this.supportsRowKind = supportsRowKind;
        this.rowKindOffset = supportsRowKind ? ROW_KIND_OFFSET : 0;
        this.fieldSerializers = (TypeSerializer<Object>[]) checkNotNull(fieldSerializers);
        this.arity = fieldSerializers.length;
        this.positionByName = positionByName;
        this.mask = new boolean[rowKindOffset + fieldSerializers.length];
        this.reuseRowPositionBased = new Row(fieldSerializers.length);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Row> duplicate() {
        TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        return new RowSerializer(duplicateFieldSerializers, positionByName, supportsRowKind);
    }

    @Override
    public Row createInstance() {
        return RowUtils.createRowWithNamedPositions(
                RowKind.INSERT, new Object[fieldSerializers.length], positionByName);
    }

    @Override
    public Row copy(Row from) {
        final Set<String> fieldNames = from.getFieldNames(false);
        if (fieldNames == null) {
            return copyPositionBased(from);
        } else {
            return copyNameBased(from, fieldNames);
        }
    }

    private Row copyPositionBased(Row from) {
        final int length = fieldSerializers.length;
        if (from.getArity() != length) {
            throw new RuntimeException(
                    "Row arity of from ("
                            + from.getArity()
                            + ") does not match "
                            + "this serializer's field length ("
                            + length
                            + ").");
        }
        final Object[] fieldByPosition = new Object[length];
        for (int i = 0; i < length; i++) {
            final Object fromField = from.getField(i);
            if (fromField != null) {
                final Object copy = fieldSerializers[i].copy(fromField);
                fieldByPosition[i] = copy;
            }
        }
        return RowUtils.createRowWithNamedPositions(
                from.getKind(), fieldByPosition, positionByName);
    }

    private Row copyNameBased(Row from, Set<String> fieldNames) {
        if (positionByName == null) {
            throw new RuntimeException("Serializer does not support named field positions.");
        }
        final Row newRow = Row.withNames(from.getKind());
        for (String fieldName : fieldNames) {
            final int targetPos = getPositionByName(fieldName);
            final Object fromField = from.getField(fieldName);
            if (fromField != null) {
                final Object copy = fieldSerializers[targetPos].copy(fromField);
                newRow.setField(fieldName, copy);
            } else {
                newRow.setField(fieldName, null);
            }
        }
        return newRow;
    }

    @Override
    public Row copy(Row from, Row reuse) {
        // cannot reuse, do a non-reuse copy
        if (reuse == null) {
            return copy(from);
        }

        final Set<String> fieldNames = from.getFieldNames(false);
        if (fieldNames == null) {
            // reuse uses name-based field mode, do a non-reuse copy
            if (reuse.getFieldNames(false) != null) {
                return copy(from);
            }
            return copyPositionBased(from, reuse);
        } else {
            // reuse uses position-based field mode, do a non-reuse copy
            if (reuse.getFieldNames(false) == null) {
                return copy(from);
            }
            return copyNameBased(from, fieldNames, reuse);
        }
    }

    private Row copyPositionBased(Row from, Row reuse) {
        final int length = fieldSerializers.length;
        if (from.getArity() != length || reuse.getArity() != length) {
            throw new RuntimeException(
                    "Row arity of reuse ("
                            + reuse.getArity()
                            + ") or from ("
                            + from.getArity()
                            + ") is "
                            + "incompatible with this serializer's field length ("
                            + length
                            + ").");
        }
        reuse.setKind(from.getKind());
        for (int i = 0; i < length; i++) {
            final Object fromField = from.getField(i);
            if (fromField != null) {
                final Object reuseField = reuse.getField(i);
                if (reuseField != null) {
                    final Object copy = fieldSerializers[i].copy(fromField, reuseField);
                    reuse.setField(i, copy);
                } else {
                    final Object copy = fieldSerializers[i].copy(fromField);
                    reuse.setField(i, copy);
                }
            } else {
                reuse.setField(i, null);
            }
        }
        return reuse;
    }

    private Row copyNameBased(Row from, Set<String> fieldNames, Row reuse) {
        if (positionByName == null) {
            throw new RuntimeException("Serializer does not support named field positions.");
        }
        reuse.clear();
        reuse.setKind(from.getKind());
        for (String fieldName : fieldNames) {
            final int targetPos = getPositionByName(fieldName);
            final Object fromField = from.getField(fieldName);
            if (fromField != null) {
                final Object reuseField = reuse.getField(fieldName);
                if (reuseField != null) {
                    final Object copy = fieldSerializers[targetPos].copy(fromField, reuseField);
                    reuse.setField(fieldName, copy);
                } else {
                    final Object copy = fieldSerializers[targetPos].copy(fromField);
                    reuse.setField(fieldName, copy);
                }
            }
        }
        return reuse;
    }

    @Override
    public int getLength() {
        return -1;
    }

    public int getArity() {
        return arity;
    }

    @Override
    public void serialize(Row record, DataOutputView target) throws IOException {
        final Set<String> fieldNames = record.getFieldNames(false);
        if (fieldNames == null) {
            serializePositionBased(record, target);
        } else {
            serializeNameBased(record, fieldNames, target);
        }
    }

    private void serializePositionBased(Row record, DataOutputView target) throws IOException {
        final int length = fieldSerializers.length;
        if (record.getArity() != length) {
            throw new RuntimeException(
                    "Row arity of record ("
                            + record.getArity()
                            + ") does not match this "
                            + "serializer's field length ("
                            + length
                            + ").");
        }

        // write bitmask
        fillMask(length, record, mask, supportsRowKind, rowKindOffset);
        writeMask(mask, target);

        // serialize non-null fields
        for (int fieldPos = 0; fieldPos < length; fieldPos++) {
            final Object o = record.getField(fieldPos);
            if (o != null) {
                fieldSerializers[fieldPos].serialize(o, target);
            }
        }
    }

    private void serializeNameBased(Row record, Set<String> fieldNames, DataOutputView target)
            throws IOException {
        if (positionByName == null) {
            throw new RuntimeException("Serializer does not support named field positions.");
        }
        reuseRowPositionBased.clear();
        reuseRowPositionBased.setKind(record.getKind());
        for (String fieldName : fieldNames) {
            final int targetPos = getPositionByName(fieldName);
            final Object value = record.getField(fieldName);
            reuseRowPositionBased.setField(targetPos, value);
        }
        serializePositionBased(reuseRowPositionBased, target);
    }

    @Override
    public Row deserialize(DataInputView source) throws IOException {
        final int length = fieldSerializers.length;

        // read bitmask
        readIntoMask(source, mask);

        // read row kind
        final RowKind kind;
        if (!supportsRowKind) {
            kind = RowKind.INSERT;
        } else {
            kind = readKindFromMask(mask);
        }

        // deserialize fields
        final Object[] fieldByPosition = new Object[length];
        for (int fieldPos = 0; fieldPos < length; fieldPos++) {
            if (!mask[rowKindOffset + fieldPos]) {
                fieldByPosition[fieldPos] = fieldSerializers[fieldPos].deserialize(source);
            }
        }

        return RowUtils.createRowWithNamedPositions(kind, fieldByPosition, positionByName);
    }

    @Override
    public Row deserialize(Row reuse, DataInputView source) throws IOException {
        // reuse uses name-based field mode, do a non-reuse deserialize
        if (reuse == null || reuse.getFieldNames(false) != null) {
            return deserialize(source);
        }
        final int length = fieldSerializers.length;

        if (reuse.getArity() != length) {
            throw new RuntimeException(
                    "Row arity of reuse ("
                            + reuse.getArity()
                            + ") does not match "
                            + "this serializer's field length ("
                            + length
                            + ").");
        }

        // read bitmask
        readIntoMask(source, mask);
        if (supportsRowKind) {
            reuse.setKind(readKindFromMask(mask));
        }

        // deserialize fields
        for (int fieldPos = 0; fieldPos < length; fieldPos++) {
            if (mask[rowKindOffset + fieldPos]) {
                reuse.setField(fieldPos, null);
            } else {
                Object reuseField = reuse.getField(fieldPos);
                if (reuseField != null) {
                    reuse.setField(
                            fieldPos, fieldSerializers[fieldPos].deserialize(reuseField, source));
                } else {
                    reuse.setField(fieldPos, fieldSerializers[fieldPos].deserialize(source));
                }
            }
        }

        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int len = fieldSerializers.length;

        // copy bitmask
        readIntoAndCopyMask(source, target, mask);

        // copy non-null fields
        for (int fieldPos = 0; fieldPos < len; fieldPos++) {
            if (!mask[rowKindOffset + fieldPos]) {
                fieldSerializers[fieldPos].copy(source, target);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSerializer that = (RowSerializer) o;
        return supportsRowKind == that.supportsRowKind
                && Arrays.equals(fieldSerializers, that.fieldSerializers);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(supportsRowKind);
        result = 31 * result + Arrays.hashCode(fieldSerializers);
        return result;
    }

    // --------------------------------------------------------------------------------------------

    private int getPositionByName(String fieldName) {
        assert positionByName != null;
        final Integer targetPos = positionByName.get(fieldName);
        if (targetPos == null) {
            throw new RuntimeException(
                    String.format(
                            "Unknown field name '%s' for mapping to a row position. "
                                    + "Available names are: %s",
                            fieldName, positionByName.keySet()));
        }
        return targetPos;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.mask = new boolean[rowKindOffset + fieldSerializers.length];
        this.reuseRowPositionBased = new Row(fieldSerializers.length);
    }

    // --------------------------------------------------------------------------------------------
    // Serialization utilities
    // --------------------------------------------------------------------------------------------

    private static void fillMask(
            int fieldLength, Row row, boolean[] mask, boolean supportsRowKind, int rowKindOffset) {
        if (supportsRowKind) {
            final byte kind = row.getKind().toByteValue();
            mask[0] = (kind & 0x01) > 0;
            mask[1] = (kind & 0x02) > 0;
        }

        for (int fieldPos = 0; fieldPos < fieldLength; fieldPos++) {
            mask[rowKindOffset + fieldPos] = row.getField(fieldPos) == null;
        }
    }

    private static RowKind readKindFromMask(boolean[] mask) {
        final byte kind = (byte) ((mask[0] ? 0x01 : 0x00) + (mask[1] ? 0x02 : 0x00));
        return RowKind.fromByteValue(kind);
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshoting & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<Row> snapshotConfiguration() {
        return new RowSerializerSnapshot(this);
    }

    /**
     * A snapshot for {@link RowSerializer}.
     *
     * @deprecated this snapshot class is no longer in use, and is maintained only for backwards
     *     compatibility. It is fully replaced by {@link RowSerializerSnapshot}.
     */
    @Deprecated
    public static final class RowSerializerConfigSnapshot
            extends CompositeTypeSerializerConfigSnapshot<Row> {

        private static final int VERSION = 1;

        /** This empty nullary constructor is required for deserializing the configuration. */
        public RowSerializerConfigSnapshot() {}

        public RowSerializerConfigSnapshot(TypeSerializer<?>[] fieldSerializers) {
            super(fieldSerializers);
        }

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public TypeSerializerSchemaCompatibility<Row> resolveSchemaCompatibility(
                TypeSerializer<Row> newSerializer) {
            TypeSerializerSnapshot<?>[] nestedSnapshots =
                    getNestedSerializersAndConfigs().stream()
                            .map(t -> t.f1)
                            .toArray(TypeSerializerSnapshot[]::new);

            return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                    newSerializer, new RowSerializerSnapshot(), nestedSnapshots);
        }
    }

    /** A {@link TypeSerializerSnapshot} for RowSerializer. */
    public static final class RowSerializerSnapshot
            extends CompositeTypeSerializerSnapshot<Row, RowSerializer> {

        private static final int VERSION = 4;

        private static final int FIRST_VERSION_WITH_ROW_KIND = 3;

        private boolean supportsRowKind = true;

        public RowSerializerSnapshot() {
            super(RowSerializer.class);
        }

        RowSerializerSnapshot(RowSerializer serializerInstance) {
            super(serializerInstance);
            this.supportsRowKind = serializerInstance.supportsRowKind;
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected void readOuterSnapshot(
                int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            if (readOuterSnapshotVersion < FIRST_VERSION_WITH_ROW_KIND) {
                supportsRowKind = false;
            } else if (readOuterSnapshotVersion == FIRST_VERSION_WITH_ROW_KIND) {
                supportsRowKind = true;
            } else {
                supportsRowKind = in.readBoolean();
            }
        }

        @Override
        protected void writeOuterSnapshot(DataOutputView out) throws IOException {
            out.writeBoolean(supportsRowKind);
        }

        @Override
        protected OuterSchemaCompatibility resolveOuterSchemaCompatibility(
                RowSerializer newSerializer) {
            if (supportsRowKind != newSerializer.supportsRowKind) {
                return OuterSchemaCompatibility.COMPATIBLE_AFTER_MIGRATION;
            }
            return OuterSchemaCompatibility.COMPATIBLE_AS_IS;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(RowSerializer outerSerializer) {
            return outerSerializer.fieldSerializers;
        }

        @Override
        protected RowSerializer createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new RowSerializer(nestedSerializers, null, supportsRowKind);
        }
    }
}
