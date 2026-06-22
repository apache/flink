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
 * Tests for the name-retaining construction path of {@link RowDataSerializer} and the V4 snapshot
 * format that carries field names. These are separate from {@link RowDataSerializerTest} because
 * they assert metadata (field names, snapshot version, V3 backward compatibility) rather than
 * per-record serialization round trips.
 */
class RowDataSerializerFieldNamesTest {

    private static RowType rowType(String[] names, LogicalType... types) {
        return RowType.of(types, names);
    }

    @Test
    void withFieldNamesRetainsTopLevelNames() {
        RowType rowType = rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType());

        assertThat(RowDataSerializer.withFieldNames(rowType).getFieldNames())
                .containsExactly("f0", "f1");
    }

    @Test
    void withFieldNamesRecursesIntoNestedRow() {
        RowType nested = rowType(new String[] {"a", "b"}, new IntType(), VarCharType.STRING_TYPE);
        RowType rowType = rowType(new String[] {"outer", "tail"}, nested, new IntType());

        RowDataSerializer serializer = RowDataSerializer.withFieldNames(rowType);
        TypeSerializer<?> nestedChild = serializer.fieldSerializers()[0];

        assertThat(nestedChild).isInstanceOf(RowDataSerializer.class);
        assertThat(((RowDataSerializer) nestedChild).getFieldNames()).containsExactly("a", "b");
    }

    @Test
    void nameLessConstructorsLeaveNamesNull() {
        RowType rowType = rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType());

        assertThat(new RowDataSerializer(rowType).getFieldNames()).isNull();
        assertThat(new RowDataSerializer(new IntType(), new BigIntType()).getFieldNames()).isNull();
    }

    @Test
    void snapshotVersionIsFour() {
        RowType rowType = rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType());

        assertThat(
                        RowDataSerializer.withFieldNames(rowType)
                                .snapshotConfiguration()
                                .getCurrentVersion())
                .isEqualTo(4);
    }

    @Test
    void namesSurviveSnapshotRoundTrip() throws IOException {
        RowType rowType = rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType());
        TypeSerializerSnapshot<RowData> snapshot =
                RowDataSerializer.withFieldNames(rowType).snapshotConfiguration();

        DataOutputSerializer out = new DataOutputSerializer(128);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        TypeSerializerSnapshot<RowData> restored =
                TypeSerializerSnapshot.readVersionedSnapshot(in, getClass().getClassLoader());

        RowDataSerializer restoredSerializer = (RowDataSerializer) restored.restoreSerializer();
        assertThat(restoredSerializer.getFieldNames()).containsExactly("f0", "f1");
    }

    @Test
    void readsV3SnapshotWithoutFieldNames() throws IOException {
        RowType rowType = rowType(new String[] {"f0", "f1"}, new IntType(), new BigIntType());
        LogicalType[] types = rowType.getChildren().toArray(new LogicalType[0]);
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

        assertThat(restored).isInstanceOf(RowDataSerializerSnapshot.class);
        assertThat(((RowDataSerializerSnapshot) restored).getFieldNames()).isNull();

        // The nested delegate was read at the correct offset (no phantom names consumed): the
        // restored serializer round-trips a sample row.
        RowDataSerializer serializer = (RowDataSerializer) restored.restoreSerializer();
        GenericRowData sample = GenericRowData.of(7, 11L);
        DataOutputSerializer rowOut = new DataOutputSerializer(32);
        serializer.serialize(sample, rowOut);
        RowData deserialized =
                serializer.deserialize(new DataInputDeserializer(rowOut.getCopyOfBuffer()));
        assertThat(deserialized.getInt(0)).isEqualTo(7);
        assertThat(deserialized.getLong(1)).isEqualTo(11L);
    }
}
