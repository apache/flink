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

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.testutils.migration.SchemaCompatibilityTestingSerializer;
import org.apache.flink.testutils.migration.SchemaCompatibilityTestingSerializer.SchemaCompatibilityTestingSnapshot;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PojoSerializerSnapshot}. */
class PojoSerializerSnapshotTest {

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

    private static final TestPojoField ID_FIELD;
    private static final TestPojoField NAME_FIELD;
    private static final TestPojoField HEIGHT_FIELD;

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
    void testRestoreSerializerWithSameFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(Arrays.asList(ID_FIELD, NAME_FIELD, HEIGHT_FIELD));

        final TypeSerializer<TestPojo> restoredSerializer = testSnapshot.restoreSerializer();
        assertThat(restoredSerializer.getClass()).isSameAs(PojoSerializer.class);
        final PojoSerializer<TestPojo> restoredPojoSerializer =
                (PojoSerializer<TestPojo>) restoredSerializer;

        final Field[] restoredFields = restoredPojoSerializer.getFields();
        assertThat(restoredFields)
                .containsExactly(ID_FIELD.field, NAME_FIELD.field, HEIGHT_FIELD.field);

        final TypeSerializer<?>[] restoredFieldSerializers =
                restoredPojoSerializer.getFieldSerializers();
        assertThat(restoredFieldSerializers)
                .containsExactly(
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        DoubleSerializer.INSTANCE);
    }

    @Test
    void testRestoreSerializerWithRemovedFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                mockRemovedField(ID_FIELD),
                                NAME_FIELD,
                                mockRemovedField(HEIGHT_FIELD)));

        final TypeSerializer<TestPojo> restoredSerializer = testSnapshot.restoreSerializer();
        assertThat(restoredSerializer).isInstanceOf(PojoSerializer.class);
        final PojoSerializer<TestPojo> restoredPojoSerializer =
                (PojoSerializer<TestPojo>) restoredSerializer;

        final Field[] restoredFields = restoredPojoSerializer.getFields();
        assertThat(restoredFields).containsExactly(null, NAME_FIELD.field, null);

        final TypeSerializer<?>[] restoredFieldSerializers =
                restoredPojoSerializer.getFieldSerializers();
        assertThat(restoredFieldSerializers)
                .containsExactly(
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        DoubleSerializer.INSTANCE);
    }

    @Test
    void testRestoreSerializerWithNewFields() {
        final PojoSerializerSnapshot<TestPojo> testSnapshot =
                buildTestSnapshot(Collections.singletonList(HEIGHT_FIELD));

        final TypeSerializer<TestPojo> restoredSerializer = testSnapshot.restoreSerializer();
        assertThat(restoredSerializer).isInstanceOf(PojoSerializer.class);
        final PojoSerializer<TestPojo> restoredPojoSerializer =
                (PojoSerializer<TestPojo>) restoredSerializer;

        final Field[] restoredFields = restoredPojoSerializer.getFields();
        assertThat(restoredFields).containsExactly(HEIGHT_FIELD.field);

        final TypeSerializer<?>[] restoredFieldSerializers =
                restoredPojoSerializer.getFieldSerializers();
        assertThat(restoredFieldSerializers).containsExactly(DoubleSerializer.INSTANCE);
    }

    // ------------------------------------------------------------------------------------------------
    //  Tests for PojoSerializerSnapshot#resolveSchemaCompatibility
    // ------------------------------------------------------------------------------------------------

    @Test
    void testResolveSchemaCompatibilityWithSameFields() {
        final PojoSerializerSnapshot<TestPojo> oldSnapshot =
                buildTestSnapshot(Arrays.asList(ID_FIELD, NAME_FIELD, HEIGHT_FIELD));

        final PojoSerializerSnapshot<TestPojo> newSnapshot =
                buildTestSnapshot(Arrays.asList(ID_FIELD, NAME_FIELD, HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                newSnapshot.resolveSchemaCompatibility(oldSnapshot);

        assertThat(resultCompatibility.isCompatibleAsIs()).isTrue();
    }

    @Test
    void testResolveSchemaCompatibilityWithRemovedFields() {
        final PojoSerializerSnapshot<TestPojo> oldSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                mockRemovedField(ID_FIELD),
                                NAME_FIELD,
                                mockRemovedField(HEIGHT_FIELD)));

        final PojoSerializerSnapshot<TestPojo> newSnapshot =
                buildTestSnapshot(Collections.singletonList(NAME_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                newSnapshot.resolveSchemaCompatibility(oldSnapshot);

        assertThat(resultCompatibility.isCompatibleAfterMigration()).isTrue();
    }

    @Test
    void testResolveSchemaCompatibilityWithNewFields() {
        final PojoSerializerSnapshot<TestPojo> oldSnapshot =
                buildTestSnapshot(Collections.singletonList(HEIGHT_FIELD));

        final PojoSerializerSnapshot<TestPojo> newSnapshot =
                buildTestSnapshot(Arrays.asList(ID_FIELD, NAME_FIELD, HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                newSnapshot.resolveSchemaCompatibility(oldSnapshot);

        assertThat(resultCompatibility.isCompatibleAfterMigration()).isTrue();
    }

    @Test
    void testResolveSchemaCompatibilityWithNewAndRemovedFields() {
        final PojoSerializerSnapshot<TestPojo> oldSnapshot =
                buildTestSnapshot(Collections.singletonList(mockRemovedField(ID_FIELD)));

        final PojoSerializerSnapshot<TestPojo> newSnapshot =
                buildTestSnapshot(Arrays.asList(NAME_FIELD, HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                newSnapshot.resolveSchemaCompatibility(oldSnapshot);

        assertThat(resultCompatibility.isCompatibleAfterMigration()).isTrue();
    }

    @Test
    void testResolveSchemaCompatibilityWithIncompatibleFieldSerializers() {
        final PojoSerializerSnapshot<TestPojo> oldSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                ID_FIELD,
                                mockFieldSerializerSnapshot(
                                        NAME_FIELD,
                                        new SchemaCompatibilityTestingSerializer()
                                                .snapshotConfiguration()),
                                HEIGHT_FIELD));

        final PojoSerializerSnapshot<TestPojo> newSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                ID_FIELD,
                                mockFieldSerializerSnapshot(
                                        NAME_FIELD,
                                        SchemaCompatibilityTestingSnapshot
                                                .thatIsIncompatibleWithTheLastSerializer()),
                                HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                newSnapshot.resolveSchemaCompatibility(oldSnapshot);

        assertThat(resultCompatibility.isIncompatible()).isTrue();
    }

    @Test
    void testResolveSchemaCompatibilityWithCompatibleAfterMigrationFieldSerializers() {
        final PojoSerializerSnapshot<TestPojo> oldSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                ID_FIELD,
                                NAME_FIELD,
                                mockFieldSerializerSnapshot(
                                        HEIGHT_FIELD,
                                        new SchemaCompatibilityTestingSerializer()
                                                .snapshotConfiguration())));

        final PojoSerializerSnapshot<TestPojo> newSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                ID_FIELD,
                                NAME_FIELD,
                                mockFieldSerializerSnapshot(
                                        HEIGHT_FIELD,
                                        SchemaCompatibilityTestingSnapshot
                                                .thatIsCompatibleWithLastSerializerAfterMigration())));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                newSnapshot.resolveSchemaCompatibility(oldSnapshot);

        assertThat(resultCompatibility.isCompatibleAfterMigration()).isTrue();
    }

    @Test
    void testResolveSchemaCompatibilityWithCompatibleWithReconfigurationFieldSerializers() {
        final PojoSerializerSnapshot<TestPojo> oldSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                mockFieldSerializerSnapshot(
                                        ID_FIELD,
                                        new SchemaCompatibilityTestingSerializer()
                                                .snapshotConfiguration()),
                                NAME_FIELD,
                                HEIGHT_FIELD));

        final PojoSerializerSnapshot<TestPojo> newSnapshot =
                buildTestSnapshot(
                        Arrays.asList(
                                mockFieldSerializerSnapshot(
                                        ID_FIELD,
                                        SchemaCompatibilityTestingSnapshot
                                                .thatIsCompatibleWithLastSerializerAfterReconfiguration()),
                                NAME_FIELD,
                                HEIGHT_FIELD));

        final TypeSerializerSchemaCompatibility<TestPojo> resultCompatibility =
                newSnapshot.resolveSchemaCompatibility(oldSnapshot);

        assertThat(resultCompatibility.isCompatibleWithReconfiguredSerializer()).isTrue();

        final TypeSerializer<TestPojo> reconfiguredSerializer =
                resultCompatibility.getReconfiguredSerializer();
        assertThat(reconfiguredSerializer.getClass()).isSameAs(PojoSerializer.class);
        final PojoSerializer<TestPojo> reconfiguredPojoSerializer =
                (PojoSerializer<TestPojo>) reconfiguredSerializer;

        final TypeSerializer<?>[] reconfiguredFieldSerializers =
                reconfiguredPojoSerializer.getFieldSerializers();
        assertThat(reconfiguredFieldSerializers)
                .containsExactly(
                        new SchemaCompatibilityTestingSerializer(),
                        StringSerializer.INSTANCE,
                        DoubleSerializer.INSTANCE);
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
                new LinkedHashMap<>(),
                new SerializerConfigImpl());
    }

    private static TestPojoField mockRemovedField(TestPojoField original) {
        TestPojoField copy = original.shallowCopy();
        copy.field = null;
        return copy;
    }

    private static TestPojoField mockFieldSerializerSnapshot(
            TestPojoField original, TypeSerializerSnapshot<?> mockSnapshot) {
        TestPojoField copy = original.shallowCopy();
        copy.serializerSnapshot = mockSnapshot;
        return copy;
    }
}
