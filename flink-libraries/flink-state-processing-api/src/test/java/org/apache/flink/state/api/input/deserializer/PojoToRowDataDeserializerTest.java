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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.api.schema.SerializerSnapshotToLogicalTypeConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PojoToRowDataDeserializer}.
 *
 * <p>Each test serializes a POJO using the real {@link PojoSerializer}, then deserializes with
 * {@link PojoToRowDataDeserializer} — no POJO class needed on the deserialization side.
 */
public class PojoToRowDataDeserializerTest {

    // -------------------------------------------------------------------------
    // POJO classes
    // -------------------------------------------------------------------------

    public static class FlatPojo {
        public String name;
        public int age;
        public long score;
        public boolean active;

        public FlatPojo() {}

        public FlatPojo(String name, int age, long score, boolean active) {
            this.name = name;
            this.age = age;
            this.score = score;
            this.active = active;
        }
    }

    public static class PojoWithNullableField {
        public String tag; // may be null
        public int value;

        public PojoWithNullableField() {}

        public PojoWithNullableField(String tag, int value) {
            this.tag = tag;
            this.value = value;
        }
    }

    public static class NestedPojo {
        public String label;
        public FlatPojo inner;

        public NestedPojo() {}

        public NestedPojo(String label, FlatPojo inner) {
            this.label = label;
            this.inner = inner;
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    public void testDeserializeFlatPojo() throws IOException {
        FlatPojo original = new FlatPojo("Alice", 30, 12345L, true);

        PojoToRowDataDeserializer deserializer = buildDeserializer(FlatPojo.class);
        GenericRowData row = (GenericRowData) roundtrip(original, FlatPojo.class, deserializer);

        assertNotNull(row);
        assertEquals(4, row.getArity());
        assertEquals(
                StringData.fromString("Alice"),
                row.getString(indexOfField(FlatPojo.class, "name")));
        assertEquals(30, row.getInt(indexOfField(FlatPojo.class, "age")));
        assertEquals(12345L, row.getLong(indexOfField(FlatPojo.class, "score")));
        assertTrue(row.getBoolean(indexOfField(FlatPojo.class, "active")));
    }

    @Test
    public void testDeserializeWithNullField() throws IOException {
        PojoWithNullableField original = new PojoWithNullableField(null, 42);
        PojoToRowDataDeserializer deserializer = buildDeserializer(PojoWithNullableField.class);
        GenericRowData row =
                (GenericRowData) roundtrip(original, PojoWithNullableField.class, deserializer);

        assertNotNull(row);
        assertTrue(row.isNullAt(indexOfField(PojoWithNullableField.class, "tag")));
        assertEquals(42, row.getInt(indexOfField(PojoWithNullableField.class, "value")));
    }

    @Test
    public void testDeserializeNullValue() throws IOException {
        TypeSerializer<FlatPojo> pojoSer = buildPojoSerializer(FlatPojo.class);
        DataOutputSerializer out = new DataOutputSerializer(64);
        pojoSer.serialize(null, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getSharedBuffer());
        PojoToRowDataDeserializer deserializer = buildDeserializer(FlatPojo.class);
        RowData result = deserializer.deserialize(in);
        assertNull(result);
    }

    @Test
    public void testDeserializeNestedPojo() throws IOException {
        NestedPojo original = new NestedPojo("outer", new FlatPojo("Bob", 25, 999L, false));
        PojoToRowDataDeserializer deserializer = buildDeserializer(NestedPojo.class);
        GenericRowData row = (GenericRowData) roundtrip(original, NestedPojo.class, deserializer);

        assertNotNull(row);
        int labelIdx = indexOfField(NestedPojo.class, "label");
        int innerIdx = indexOfField(NestedPojo.class, "inner");
        assertEquals(StringData.fromString("outer"), row.getString(labelIdx));

        // Nested POJO should be a GenericRowData
        RowData innerRow = row.getRow(innerIdx, 4);
        assertNotNull(innerRow);
    }

    @Test
    public void testUnregisteredSubclassThrowsIoException() throws IOException {
        // Write a value normally using the serializer, then inject fake IS_SUBCLASS bytes.
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeByte(PojoSerializer.IS_SUBCLASS);
        out.writeUTF("com.example.UnknownSubclass");

        DataInputDeserializer in = new DataInputDeserializer(out.getSharedBuffer());
        PojoToRowDataDeserializer deserializer = buildDeserializer(FlatPojo.class);

        IOException e = assertThrows(IOException.class, () -> deserializer.deserialize(in));
        assertTrue(e.getMessage().contains("UnknownSubclass"));
    }

    @Test
    public void testTaggedSubclassWithUnresolvableDeserializerThrowsIoException()
            throws IOException {
        // A registered subclass whose own serializer snapshot could not be turned into a
        // PojoToRowDataDeserializer (e.g. it is not a POJO, or its snapshot was unreadable) is
        // represented by a `null` entry in registeredSubclassDeserializers (see
        // PojoSerializerSnapshot#getRegisteredSubclassSnapshotsOrdered). Deserializing a tagged
        // subclass that resolves to such an entry must fail with a clear IOException rather than
        // an NPE.
        List<PojoToRowDataDeserializer> registeredSubclassDeserializers = new ArrayList<>();
        registeredSubclassDeserializers.add(null);
        PojoToRowDataDeserializer deserializer =
                new PojoToRowDataDeserializer(
                        new TypeSerializer[0],
                        new LogicalType[0],
                        new String[0],
                        registeredSubclassDeserializers);

        DataOutputSerializer out = new DataOutputSerializer(8);
        out.writeByte(PojoSerializer.IS_TAGGED_SUBCLASS);
        out.writeByte(0);
        DataInputDeserializer in = new DataInputDeserializer(out.getSharedBuffer());

        IOException e = assertThrows(IOException.class, () -> deserializer.deserialize(in));
        assertTrue(e.getMessage().contains("tag 0"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static <T> PojoSerializer<T> buildPojoSerializer(Class<T> clazz) {
        return (PojoSerializer<T>)
                TypeExtractor.createTypeInfo(clazz).createSerializer(new SerializerConfigImpl());
    }

    @SuppressWarnings("unchecked")
    private static <T> PojoToRowDataDeserializer buildDeserializer(Class<T> clazz) {
        PojoSerializer<T> ser = buildPojoSerializer(clazz);
        PojoSerializerSnapshot<T> snapshot =
                (PojoSerializerSnapshot<T>) ser.snapshotConfiguration();
        return PojoToRowDataDeserializer.create(snapshot);
    }

    private static <T> RowData roundtrip(T value, Class<T> clazz, PojoToRowDataDeserializer deser)
            throws IOException {
        PojoSerializer<T> ser = buildPojoSerializer(clazz);
        DataOutputSerializer out = new DataOutputSerializer(256);
        ser.serialize(value, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getSharedBuffer());
        return deser.deserialize(in);
    }

    /** Returns the field index as it appears in the PojoSerializer's field ordering. */
    private static int indexOfField(Class<?> clazz, String fieldName) {
        RowType rowType =
                (RowType)
                        SerializerSnapshotToLogicalTypeConverter.convert(
                                buildPojoSerializer(clazz).snapshotConfiguration());
        int idx = rowType.getFieldNames().indexOf(fieldName);
        assertTrue(idx >= 0, "Field '" + fieldName + "' not found");
        return idx;
    }
}
