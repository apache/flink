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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.testutils.migration.SchemaCompatibilityTestingSerializer;
import org.apache.flink.testutils.migration.SchemaCompatibilityTestingSerializer.SchemaCompatibilityTestingSnapshot;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link PojoSerializerSnapshot}. */
public class PojoSerializerSnapshotTest {

    public static class TestPojo {
        public int id;
        public String name;
        public double height;

        public TestPojo() {}

        public TestPojo(int id, String name, double height) {
            this.id = id;
            this.name = name;
            this.height = height;
        }
    }

    private static class TestPojoField {
        String name;
        Field field;
        TypeSerializer<?> serializer;
        TypeSerializerSnapshot<?> serializerSnapshot;

        TestPojoField(String name, TypeSerializer<?> serializer) throws Exception {
            this.name = name;
            this.field = TestPojo.class.getDeclaredField(name);
            this.serializer = serializer;
            this.serializerSnapshot = serializer.snapshotConfiguration();
        }

        TestPojoField(String name, Field field, TypeSerializerSnapshot<?> serializerSnapshot) {
            this.name = name;
            this.field = field;
            this.serializerSnapshot = serializerSnapshot;
        }

        TestPojoField shallowCopy() {
            return new TestPojoField(name, field, serializerSnapshot);
        }
    }

    private static final Map<String, Field> FIELDS = new HashMap<>(3);

    static {
        try {
            FIELDS.put("id", TestPojo.class.getDeclaredField("id"));
            FIELDS.put("name", TestPojo.class.getDeclaredField("name"));
            FIELDS.put("height", TestPojo.class.getDeclaredField("height"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static TestPojoField ID_FIELD;
    private static TestPojoField NAME_FIELD;
    private static TestPojoField HEIGHT_FIELD;

    static {
        try {
            ID_FIELD = new TestPojoField("id", IntSerializer.INSTANCE);
            NAME_FIELD = new TestPojoField("name", StringSerializer.INSTANCE);
            HEIGHT_FIELD = new TestPojoField("height", DoubleSerializer.INSTANCE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    // ------------------------------------------------------------------------------------------------
    //  Tests for PojoSerializerSnapshot#restoreSerializer
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testRestoreSerializerWithSameFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(Arrays.asList(ID_FIELD, NAME_FIELD, HEIGHT_FIELD));

        final TypeSerializer<TestPojo> restoredSerializer = testSnapshot.restoreSerializer();
        assertSame(restoredSerializer.getClass(), PojoSerializer.class);
        final PojoSerializer<TestPojo> restoredPojoSerializer =
                (PojoSerializer<TestPojo>) restoredSerializer;

        final Field[] restoredFields = restoredPojoSerializer.getFields();
        assertArrayEquals(
                new Field[] {ID_FIELD.field, NAME_FIELD.field, HEIGHT_FIELD.field}, restoredFields);

        final TypeSerializer<?>[] restoredFieldSerializers =
                restoredPojoSerializer.getFieldSerializers();
        assertArrayEquals(
                new TypeSerializer[] {
                    IntSerializer.INSTANCE, StringSerializer.INSTANCE, DoubleSerializer.INSTANCE
                },
                restoredFieldSerializers);
    }

    @Test
    public void testRestoreSerializerWithRemovedFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                mockRemovedField(ID_FIELD),
                                NAME_FIELD,
                                mockRemovedField(HEIGHT_FIELD)));

        final TypeSerializer<TestPojo> restoredSerializer = testSnapshot.restoreSerializer();
        assertTrue(restoredSerializer.getClass() == PojoSerializer.class);
        final PojoSerializer<TestPojo> restoredPojoSerializer =
                (PojoSerializer<TestPojo>) restoredSerializer;

        final Field[] restoredFields = restoredPojoSerializer.getFields();
        assertArrayEquals(new Field[] {null, NAME_FIELD.field, null}, restoredFields);

        final TypeSerializer<?>[] restoredFieldSerializers =
                restoredPojoSerializer.getFieldSerializers();
        assertArrayEquals(
                new TypeSerializer[] {
                    IntSerializer.INSTANCE, StringSerializer.INSTANCE, DoubleSerializer.INSTANCE
                },
                restoredFieldSerializers);
    }

    @Test
    public void testRestoreSerializerWithNewFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(Collections.singletonList(HEIGHT_FIELD));

        final TypeSerializer<TestPojo> restoredSerializer = testSnapshot.restoreSerializer();
        assertTrue(restoredSerializer.getClass() == PojoSerializer.class);
        final PojoSerializer<TestPojo> restoredPojoSerializer =
                (PojoSerializer<TestPojo>) restoredSerializer;

        final Field[] restoredFields = restoredPojoSerializer.getFields();
        assertArrayEquals(new Field[] {HEIGHT_FIELD.field}, restoredFields);

        final TypeSerializer<?>[] restoredFieldSerializers =
                restoredPojoSerializer.getFieldSerializers();
        assertArrayEquals(
                new TypeSerializer[] {DoubleSerializer.INSTANCE}, restoredFieldSerializers);
    }

    // ------------------------------------------------------------------------------------------------
    //  Tests for PojoSerializerSnapshot#resolveSchemaCompatibility
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testResolveSchemaCompatibilityWithSameFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(Arrays.asList(ID_FIELD, NAME_FIELD, HEIGHT_FIELD));

        final PojoSerializer<TestPojo> newPojoSerializer =
                buildTestNewPojoSerializer(Arrays.asList(ID_FIELD, NAME_FIELD, HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                testSnapshot.resolveSchemaCompatibility(newPojoSerializer);

        assertTrue(resultCompatibility.isCompatibleAsIs());
    }

    @Test
    public void testResolveSchemaCompatibilityWithRemovedFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                mockRemovedField(ID_FIELD),
                                NAME_FIELD,
                                mockRemovedField(HEIGHT_FIELD)));

        final PojoSerializer<TestPojo> newPojoSerializer =
                buildTestNewPojoSerializer(Collections.singletonList(NAME_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                testSnapshot.resolveSchemaCompatibility(newPojoSerializer);

        assertTrue(resultCompatibility.isCompatibleAfterMigration());
    }

    @Test
    public void testResolveSchemaCompatibilityWithNewFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(Collections.singletonList(HEIGHT_FIELD));

        final PojoSerializer<TestPojo> newPojoSerializer =
                buildTestNewPojoSerializer(Arrays.asList(ID_FIELD, NAME_FIELD, HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                testSnapshot.resolveSchemaCompatibility(newPojoSerializer);

        assertTrue(resultCompatibility.isCompatibleAfterMigration());
    }

    @Test
    public void testResolveSchemaCompatibilityWithNewAndRemovedFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(Collections.singletonList(mockRemovedField(ID_FIELD)));

        final PojoSerializer<TestPojo> newPojoSerializer =
                buildTestNewPojoSerializer(Arrays.asList(NAME_FIELD, HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                testSnapshot.resolveSchemaCompatibility(newPojoSerializer);

        assertTrue(resultCompatibility.isCompatibleAfterMigration());
    }

    @Test
    public void testResolveSchemaCompatibilityWithIncompatibleFieldSerializers() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                ID_FIELD,
                                mockFieldSerializerSnapshot(
                                        NAME_FIELD,
                                        SchemaCompatibilityTestingSnapshot
                                                .thatIsIncompatibleWithTheNextSerializer()),
                                HEIGHT_FIELD));

        final PojoSerializer<TestPojo> newPojoSerializer =
                buildTestNewPojoSerializer(
                        Arrays.asList(
                                ID_FIELD,
                                mockFieldSerializer(
                                        NAME_FIELD, new SchemaCompatibilityTestingSerializer()),
                                HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                testSnapshot.resolveSchemaCompatibility(newPojoSerializer);

        assertTrue(resultCompatibility.isIncompatible());
    }

    @Test
    public void testResolveSchemaCompatibilityWithCompatibleAfterMigrationFieldSerializers() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                ID_FIELD,
                                NAME_FIELD,
                                mockFieldSerializerSnapshot(
                                        HEIGHT_FIELD,
                                        SchemaCompatibilityTestingSnapshot
                                                .thatIsCompatibleWithNextSerializerAfterMigration())));

        final PojoSerializer<TestPojo> newPojoSerializer =
                buildTestNewPojoSerializer(
                        Arrays.asList(
                                ID_FIELD,
                                NAME_FIELD,
                                mockFieldSerializer(
                                        HEIGHT_FIELD, new SchemaCompatibilityTestingSerializer())));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                testSnapshot.resolveSchemaCompatibility(newPojoSerializer);

        assertTrue(resultCompatibility.isCompatibleAfterMigration());
    }

    @Test
    public void testResolveSchemaCompatibilityWithCompatibleWithReconfigurationFieldSerializers() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                mockFieldSerializerSnapshot(
                                        ID_FIELD,
                                        SchemaCompatibilityTestingSnapshot
                                                .thatIsCompatibleWithNextSerializerAfterReconfiguration()),
                                NAME_FIELD,
                                HEIGHT_FIELD));

        final PojoSerializer<TestPojo> newPojoSerializer =
                buildTestNewPojoSerializer(
                        Arrays.asList(
                                mockFieldSerializer(
                                        ID_FIELD, new SchemaCompatibilityTestingSerializer()),
                                NAME_FIELD,
                                HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                testSnapshot.resolveSchemaCompatibility(newPojoSerializer);

        assertTrue(resultCompatibility.isCompatibleWithReconfiguredSerializer());

        final TypeSerializer<TestPojo> reconfiguredSerializer =
                resultCompatibility.getReconfiguredSerializer();
        assertSame(reconfiguredSerializer.getClass(), PojoSerializer.class);
        final PojoSerializer<TestPojo> reconfiguredPojoSerializer =
                (PojoSerializer<TestPojo>) reconfiguredSerializer;

        final TypeSerializer<?>[] reconfiguredFieldSerializers =
                reconfiguredPojoSerializer.getFieldSerializers();
        assertArrayEquals(
                new TypeSerializer[] {
                    new SchemaCompatibilityTestingSerializer(),
                    StringSerializer.INSTANCE,
                    DoubleSerializer.INSTANCE
                },
                reconfiguredFieldSerializers);
    }

    // ------------------------------------------------------------------------------------------------
    //  Test utilities
    // ------------------------------------------------------------------------------------------------

    private static PojoSerializerSnapshot<TestPojo> buildTestSnapshot(
            List<TestPojoField> fieldsToContainInSnapshot) {

        int numFields = fieldsToContainInSnapshot.size();
        ArrayList<Field> fields = new ArrayList<>(numFields);
        ArrayList<TypeSerializerSnapshot<?>> fieldSerializerSnapshots = new ArrayList<>(numFields);
        fieldsToContainInSnapshot.forEach(
                testPojoField -> {
                    fields.add(testPojoField.field);
                    fieldSerializerSnapshots.add(testPojoField.serializerSnapshot);
                });

        return new PojoSerializerSnapshot<>(
                TestPojo.class,
                fields.toArray(new Field[numFields]),
                fieldSerializerSnapshots.toArray(new TypeSerializerSnapshot[numFields]),
                new LinkedHashMap<>(),
                new LinkedHashMap<>());
    }

    private static PojoSerializer<TestPojo> buildTestNewPojoSerializer(
            List<TestPojoField> fieldsForNewPojo) {
        int numFields = fieldsForNewPojo.size();

        final ArrayList<Field> fields = new ArrayList<>(numFields);
        final ArrayList<TypeSerializer<?>> fieldSerializers = new ArrayList<>(numFields);
        fieldsForNewPojo.forEach(
                fieldForNewPojo -> {
                    fields.add(fieldForNewPojo.field);
                    fieldSerializers.add(fieldForNewPojo.serializer);
                });

        return new PojoSerializer<>(
                TestPojo.class,
                fieldSerializers.toArray(new TypeSerializer[numFields]),
                fields.toArray(new Field[numFields]),
                new ExecutionConfig());
    }

    private static TestPojoField mockRemovedField(TestPojoField original) {
        TestPojoField copy = original.shallowCopy();
        copy.field = null;
        return copy;
    }

    private static TestPojoField mockFieldSerializer(
            TestPojoField original, TypeSerializer<?> mockSerializer) {
        TestPojoField copy = original.shallowCopy();
        copy.serializer = mockSerializer;
        return copy;
    }

    private static TestPojoField mockFieldSerializerSnapshot(
            TestPojoField original, TypeSerializerSnapshot<?> mockSnapshot) {
        TestPojoField copy = original.shallowCopy();
        copy.serializerSnapshot = mockSnapshot;
        return copy;
    }
}
