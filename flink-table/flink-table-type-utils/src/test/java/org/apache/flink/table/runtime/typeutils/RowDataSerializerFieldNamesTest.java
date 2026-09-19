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

import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer.RowDataSerializerSnapshot;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the top-level field names carried by {@link RowDataSerializer} and the V4 {@link
 * RowDataSerializerSnapshot} format that persists them unconditionally. These assert metadata
 * (field names, snapshot version, V3 backward compatibility, compatibility invariance) rather than
 * per-record serialization round trips, which are covered by {@link RowDataSerializerTest}.
 */
class RowDataSerializerFieldNamesTest {

    private static RowType rowType(String[] names, LogicalType... types) {
        return RowType.of(types, names);
    }

    @Test
    void internalSerializersCreateCarriesTopLevelNames() {
        RowType rowType = rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType());

        assertThat(InternalSerializers.create(rowType).getFieldNames()).containsExactly("f0", "f1");
    }

    @Test
    void typesOnlyConstructorsLeaveNamesNull() {
        assertThat(new RowDataSerializer(new IntType(), new BigIntType()).getFieldNames()).isNull();
        assertThat(
                        new RowDataSerializer(
                                        new LogicalType[] {new IntType(), new BigIntType()},
                                        new TypeSerializer[] {
                                            InternalSerializers.create(new IntType()),
                                            InternalSerializers.create(new BigIntType())
                                        })
                                .getFieldNames())
                .isNull();
    }

    @Test
    void duplicatePreservesNames() {
        RowType rowType = rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType());
        RowDataSerializer duplicate =
                (RowDataSerializer) InternalSerializers.create(rowType).duplicate();

        assertThat(duplicate.getFieldNames()).containsExactly("f0", "f1");
    }

    @Test
    void snapshotVersionIsFour() {
        RowType rowType = rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType());

        assertThat(InternalSerializers.create(rowType).snapshotConfiguration().getCurrentVersion())
                .isEqualTo(4);
    }

    @Test
    void namesSurviveSnapshotRoundTripIncludingNestedRow() throws IOException {
        RowType nested = rowType(new String[] {"a", "b"}, new IntType(), VarCharType.STRING_TYPE);
        RowType rowType = rowType(new String[] {"outer", "tail"}, nested, new IntType());

        RowDataSerializer restored =
                (RowDataSerializer)
                        roundTrip(InternalSerializers.create(rowType).snapshotConfiguration())
                                .restoreSerializer();

        assertThat(restored.getFieldNames()).containsExactly("outer", "tail");
        TypeSerializer<?> nestedChild = restored.fieldSerializers()[0];
        assertThat(nestedChild).isInstanceOf(RowDataSerializer.class);
        assertThat(((RowDataSerializer) nestedChild).getFieldNames()).containsExactly("a", "b");
    }

    @Test
    void readsV3SnapshotWithoutFieldNames() throws IOException {
        LogicalType[] types = {new IntType(), new BigIntType()};
        TypeSerializer<?>[] fieldSerializers = {
            InternalSerializers.create(types[0]), InternalSerializers.create(types[1])
        };

        // Hand-craft a genuine V3 stream: versioned header + V3 body, where the V3 body is the
        // types section followed directly by the nested-delegate section, with no names block.
        DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeUTF(RowDataSerializerSnapshot.class.getName());
        out.writeInt(3);
        out.writeInt(types.length);
        DataOutputViewStream stream = new DataOutputViewStream(out);
        for (LogicalType type : types) {
            InstantiationUtil.serializeObject(stream, type);
        }
        new NestedSerializersSnapshotDelegate(fieldSerializers).writeNestedSerializerSnapshots(out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        TypeSerializerSnapshot<RowData> restored =
                TypeSerializerSnapshot.readVersionedSnapshot(in, getClass().getClassLoader());

        RowDataSerializer serializer = (RowDataSerializer) restored.restoreSerializer();
        assertThat(serializer.getFieldNames()).isNull();

        // The nested delegate was read at the correct offset (no phantom names consumed): the
        // restored serializer round-trips a sample row.
        GenericRowData sample = GenericRowData.of(7, 11L);
        DataOutputSerializer rowOut = new DataOutputSerializer(32);
        serializer.serialize(sample, rowOut);
        RowData deserialized =
                serializer.deserialize(new DataInputDeserializer(rowOut.getCopyOfBuffer()));
        assertThat(deserialized.getInt(0)).isEqualTo(7);
        assertThat(deserialized.getLong(1)).isEqualTo(11L);
    }

    @Test
    void equalsAndHashCodeIgnoreFieldNames() {
        RowDataSerializer named =
                InternalSerializers.create(
                        rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType()));
        RowDataSerializer differentlyNamed =
                InternalSerializers.create(
                        rowType(new String[] {"x", "y"}, new IntType(), new BigIntType()));

        assertThat(named).isEqualTo(differentlyNamed);
        assertThat(named.hashCode()).isEqualTo(differentlyNamed.hashCode());
    }

    @Test
    void differentFieldNamesRemainCompatibleWhenTypesMatch() {
        TypeSerializerSchemaCompatibility<RowData> compatibility =
                resolve(
                        rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType()),
                        rowType(new String[] {"x", "y"}, new IntType(), new BigIntType()));

        assertThat(compatibility.isCompatibleAsIs()).isTrue();
    }

    @Test
    void differentTypesRemainIncompatibleRegardlessOfNames() {
        TypeSerializerSchemaCompatibility<RowData> compatibility =
                resolve(
                        rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType()),
                        rowType(new String[] {"f0", "f1"}, new IntType(), new IntType()));

        assertThat(compatibility.isIncompatible()).isTrue();
    }

    private static TypeSerializerSchemaCompatibility<RowData> resolve(
            RowType newType, RowType oldType) {
        TypeSerializerSnapshot<RowData> newSnapshot =
                InternalSerializers.create(newType).snapshotConfiguration();
        TypeSerializerSnapshot<RowData> oldSnapshot =
                InternalSerializers.create(oldType).snapshotConfiguration();
        return newSnapshot.resolveSchemaCompatibility(oldSnapshot);
    }

    private static TypeSerializerSnapshot<RowData> roundTrip(
            TypeSerializerSnapshot<RowData> snapshot) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(128);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        return TypeSerializerSnapshot.readVersionedSnapshot(
                in, RowDataSerializerFieldNamesTest.class.getClassLoader());
    }
}
