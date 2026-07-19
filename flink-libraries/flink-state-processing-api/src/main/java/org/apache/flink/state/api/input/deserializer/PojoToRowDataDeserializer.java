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

package org.apache.flink.state.api.input.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoDeserializerCompatibilitySnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.state.api.schema.SerializerSnapshotToLogicalTypeConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link TypeSerializer} that reads the POJO binary format written by {@link
 * org.apache.flink.api.java.typeutils.runtime.PojoSerializer} and produces {@link GenericRowData}.
 *
 * <p>This deserializer does <em>not</em> require the user POJO class to be on the classpath. It
 * mirrors the exact binary protocol of {@code PojoSerializer}:
 *
 * <pre>{@code
 * 1 byte: flags (bitmask)
 *   0x01 IS_NULL            → value is null, return null
 *   0x02 NO_SUBCLASS        → exact POJO class: read numFields × (isNull boolean + field bytes)
 *   0x08 IS_TAGGED_SUBCLASS → 1 byte subclass tag; delegate to registered subclass deserializer
 *   0x04 IS_SUBCLASS        → UTF class name (must be read); Kryo not supported → throws IOException
 * }</pre>
 *
 * <p>Use {@link #create(PojoSerializerSnapshot)} to build an instance from a savepoint snapshot.
 */
@Internal
public final class PojoToRowDataDeserializer extends TypeSerializer<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PojoToRowDataDeserializer.class);

    // Mirrors constants in PojoSerializer
    static final int IS_NULL = 0x01;
    static final int NO_SUBCLASS = 0x02;
    static final int IS_SUBCLASS = 0x04;
    static final int IS_TAGGED_SUBCLASS = 0x08;

    private final int numFields;
    private final TypeSerializer<?>[] fieldDeserializers;
    private final LogicalType[] fieldTypes;
    private final String[] fieldNames;
    private final List<PojoToRowDataDeserializer> registeredSubclassDeserializers;

    /**
     * Builds a {@link PojoToRowDataDeserializer} from a {@link PojoSerializerSnapshot}.
     *
     * <p>For each field:
     *
     * <ul>
     *   <li>If the field snapshot is itself a {@link PojoSerializerSnapshot}, this method recurses
     *       to build a nested {@link PojoToRowDataDeserializer}.
     *   <li>For all other field types, the field's original serializer is restored via {@link
     *       TypeSerializerSnapshot#restoreSerializer()}.
     * </ul>
     *
     * <p>Registered subclasses are handled by building a deserializer for each registered subclass
     * snapshot in order (matching the tag index used in the binary format).
     *
     * @throws IllegalStateException if a required field serializer snapshot is absent
     */
    public static PojoToRowDataDeserializer create(PojoSerializerSnapshot<?> snapshot) {
        List<AbstractMap.SimpleEntry<String, TypeSerializerSnapshot<?>>> fieldEntries =
                snapshot.getFieldSnapshotEntries();

        List<TypeSerializer<?>> fieldDeserializerList = new ArrayList<>(fieldEntries.size());
        List<LogicalType> fieldTypeList = new ArrayList<>(fieldEntries.size());
        List<String> fieldNameList = new ArrayList<>(fieldEntries.size());

        for (AbstractMap.SimpleEntry<String, TypeSerializerSnapshot<?>> entry : fieldEntries) {
            String fieldName = entry.getKey();
            TypeSerializerSnapshot<?> fieldSnapshot = entry.getValue();

            if (fieldSnapshot == null) {
                throw new IllegalStateException(
                        "Cannot build deserializer for field '"
                                + fieldName
                                + "': its serializer snapshot was not readable from the savepoint. "
                                + "This field cannot be deserialized without the original snapshot.");
            }

            TypeSerializer<?> fieldDeserializer;
            if (fieldSnapshot instanceof PojoSerializerSnapshot) {
                fieldDeserializer = create((PojoSerializerSnapshot<?>) fieldSnapshot);
            } else {
                fieldDeserializer = fieldSnapshot.restoreSerializer();
            }

            fieldDeserializerList.add(fieldDeserializer);
            fieldTypeList.add(SerializerSnapshotToLogicalTypeConverter.convert(fieldSnapshot));
            fieldNameList.add(fieldName);
        }

        List<TypeSerializerSnapshot<?>> subSnapshots =
                snapshot.getRegisteredSubclassSnapshotsOrdered();
        List<PojoToRowDataDeserializer> subDeserializers = new ArrayList<>(subSnapshots.size());
        for (TypeSerializerSnapshot<?> subSnap : subSnapshots) {
            subDeserializers.add(
                    subSnap instanceof PojoSerializerSnapshot
                            ? create((PojoSerializerSnapshot<?>) subSnap)
                            : null);
        }

        return new PojoToRowDataDeserializer(
                fieldDeserializerList.toArray(new TypeSerializer[0]),
                fieldTypeList.toArray(new LogicalType[0]),
                fieldNameList.toArray(new String[0]),
                subDeserializers);
    }

    PojoToRowDataDeserializer(
            TypeSerializer<?>[] fieldDeserializers,
            LogicalType[] fieldTypes,
            String[] fieldNames,
            List<PojoToRowDataDeserializer> registeredSubclassDeserializers) {
        this.numFields = fieldDeserializers.length;
        this.fieldDeserializers = fieldDeserializers;
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames;
        this.registeredSubclassDeserializers = registeredSubclassDeserializers;
    }

    @Override
    public RowData deserialize(DataInputView source) throws IOException {
        int flags = source.readByte() & 0xFF;

        if ((flags & IS_NULL) != 0) {
            return null;
        }

        if ((flags & NO_SUBCLASS) != 0) {
            return readFields(source);
        }

        if ((flags & IS_TAGGED_SUBCLASS) != 0) {
            int tag = source.readByte() & 0xFF;
            if (tag < registeredSubclassDeserializers.size()) {
                return registeredSubclassDeserializers.get(tag).deserialize(source);
            }
            throw new IOException(
                    "Unknown registered subclass tag "
                            + tag
                            + " (have "
                            + registeredSubclassDeserializers.size()
                            + " registered). The savepoint may have been written with more subclasses registered.");
        }

        if ((flags & IS_SUBCLASS) != 0) {
            String className = source.readUTF();
            throw new IOException(
                    "Cannot deserialize POJO subclass '"
                            + className
                            + "': the subclass uses Kryo serialization, which requires the class on the"
                            + " classpath. Kryo-encoded bytes have unknown length and cannot be skipped."
                            + " Register the subclass or add the JAR to the classpath.");
        }

        throw new IOException("Unrecognised POJO flags byte: 0x" + Integer.toHexString(flags));
    }

    @Override
    public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    private GenericRowData readFields(DataInputView source) throws IOException {
        GenericRowData row = new GenericRowData(numFields);
        for (int i = 0; i < numFields; i++) {
            boolean isNull = source.readBoolean();
            if (isNull) {
                row.setField(i, null);
                continue;
            }
            Object raw;
            try {
                raw = fieldDeserializers[i].deserialize(source);
            } catch (Exception e) {
                // Stream position may be corrupted — null remaining fields and return partial row.
                LOG.warn(
                        "Failed to deserialize field '{}' (index {}): {}. "
                                + "Setting this and remaining fields to null.",
                        fieldNames[i],
                        i,
                        e.getMessage());
                row.setField(i, null);
                for (int j = i + 1; j < numFields; j++) {
                    row.setField(j, null);
                }
                return row;
            }
            try {
                row.setField(i, InternalTypeConverter.toInternal(raw, fieldTypes[i]));
            } catch (Exception e) {
                // Bytes already consumed correctly; only this field's conversion failed.
                LOG.warn(
                        "Failed to convert field '{}' (index {}) value: {}. Setting field to null.",
                        fieldNames[i],
                        i,
                        e.getMessage());
                row.setField(i, null);
            }
        }
        return row;
    }

    // -------------------------------------------------------------------------
    // TypeSerializer boilerplate — copy/snapshot operations not needed for
    // schema-extraction use cases but required by the interface.
    // -------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<RowData> duplicate() {
        return this;
    }

    @Override
    public RowData createInstance() {
        return new GenericRowData(numFields);
    }

    @Override
    public RowData copy(RowData from) {
        return from;
    }

    @Override
    public RowData copy(RowData from, RowData reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(RowData record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException(
                "PojoToRowDataDeserializer is read-only; serialization is not supported.");
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException(
                "PojoToRowDataDeserializer is read-only; copy is not supported.");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PojoToRowDataDeserializer)) {
            return false;
        }
        PojoToRowDataDeserializer other = (PojoToRowDataDeserializer) obj;
        return numFields == other.numFields;
    }

    @Override
    public int hashCode() {
        return numFields;
    }

    @Override
    public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
        return new PojoDeserializerCompatibilitySnapshot<>(this);
    }
}
