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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshotTest;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;

/**
 * Test suite for {@link
 * org.apache.flink.streaming.api.utils.LengthPrefixWrapperSerializer.LengthPrefixWrapperSerializerSnapshot}.
 */
public class LengthPrefixWrapperSerializerSnapshotTest {

    // ------------------------------------------------------------------------------------------------
    //  Scope: tests LengthPrefixWrapperSerializerSnapshot#resolveSchemaCompatibility
    // ------------------------------------------------------------------------------------------------

    @Test
    void testIncompatible() throws Exception {
        TypeSerializer<String> nestedSerializer =
                createNestedSerializer(
                        CompositeTypeSerializerSnapshotTest.TargetCompatibility.INCOMPATIBLE);
        TypeSerializerSchemaCompatibility<String> schemaCompatibility =
                snapshotSerializerAndGetSchemaCompatibilityAfterRestore(
                        nestedSerializer, nestedSerializer);
        Assertions.assertTrue(schemaCompatibility.isIncompatible());
    }

    @Test
    void testCompatibleAsIs() throws Exception {
        TypeSerializer<String> nestedSerializer =
                createNestedSerializer(
                        CompositeTypeSerializerSnapshotTest.TargetCompatibility.COMPATIBLE_AS_IS);
        TypeSerializerSchemaCompatibility<String> schemaCompatibility =
                snapshotSerializerAndGetSchemaCompatibilityAfterRestore(
                        nestedSerializer, nestedSerializer);
        Assertions.assertTrue(schemaCompatibility.isCompatibleAsIs());
    }

    @Test
    void testCompatibleAfterMigration() throws Exception {
        TypeSerializer<String> nestedSerializer =
                createNestedSerializer(
                        CompositeTypeSerializerSnapshotTest.TargetCompatibility
                                .COMPATIBLE_AFTER_MIGRATION);
        TypeSerializerSchemaCompatibility<String> schemaCompatibility =
                snapshotSerializerAndGetSchemaCompatibilityAfterRestore(
                        nestedSerializer, nestedSerializer);
        Assertions.assertTrue(schemaCompatibility.isCompatibleAfterMigration());
    }

    @Test
    void testCompatibleWithReconfiguredSerializer() throws Exception {
        TypeSerializer<String> nestedSerializer =
                createNestedSerializer(
                        CompositeTypeSerializerSnapshotTest.TargetCompatibility
                                .COMPATIBLE_WITH_RECONFIGURED_SERIALIZER);
        TypeSerializerSchemaCompatibility<String> schemaCompatibility =
                snapshotSerializerAndGetSchemaCompatibilityAfterRestore(
                        nestedSerializer, nestedSerializer);
        Assertions.assertTrue(schemaCompatibility.isCompatibleWithReconfiguredSerializer());
        TypeSerializer<String> reconfiguredSerializer =
                schemaCompatibility.getReconfiguredSerializer();
        Assertions.assertSame(
                reconfiguredSerializer.getClass(), LengthPrefixWrapperSerializer.class);
        Assertions.assertEquals(
                ((LengthPrefixWrapperSerializer<String>) reconfiguredSerializer)
                        .getDataSerializer()
                        .getClass()
                        .getSimpleName(),
                "ReconfiguredNestedSerializer");
    }

    private TypeSerializerSchemaCompatibility<String>
            snapshotSerializerAndGetSchemaCompatibilityAfterRestore(
                    TypeSerializer<String> initialNestedSerializer,
                    TypeSerializer<String> newNestedSerializer)
                    throws IOException {
        TypeSerializer<String> testSerializer =
                new LengthPrefixWrapperSerializer<>(initialNestedSerializer);

        TypeSerializerSnapshot<String> testSerializerSnapshot =
                testSerializer.snapshotConfiguration();

        DataOutputSerializer out = new DataOutputSerializer(128);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, testSerializerSnapshot);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        testSerializerSnapshot =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        in, Thread.currentThread().getContextClassLoader());

        TypeSerializer<String> newTestSerializer =
                new LengthPrefixWrapperSerializer<>(newNestedSerializer);
        return testSerializerSnapshot.resolveSchemaCompatibility(newTestSerializer);
    }

    // ------------------------------------------------------------------------------------------------
    //  Scope: tests LengthPrefixWrapperSerializerSnapshot#restoreSerializer
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testRestoreSerializer() throws Exception {
        TypeSerializer<String> testNestedSerializer =
                createNestedSerializer(
                        CompositeTypeSerializerSnapshotTest.TargetCompatibility.COMPATIBLE_AS_IS);

        LengthPrefixWrapperSerializer<String> testSerializer =
                new LengthPrefixWrapperSerializer<>(testNestedSerializer);

        TypeSerializerSnapshot<String> testSerializerSnapshot =
                testSerializer.snapshotConfiguration();

        DataOutputSerializer out = new DataOutputSerializer(128);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, testSerializerSnapshot);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        testSerializerSnapshot =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        in, Thread.currentThread().getContextClassLoader());

        testSerializer =
                (LengthPrefixWrapperSerializer<String>) testSerializerSnapshot.restoreSerializer();
        Assertions.assertEquals(
                testSerializer.getDataSerializer().getClass().getSimpleName(),
                "RestoredNestedSerializer");
    }

    // ------------------------------------------------------------------------------------------------
    //  Test utilities
    // ------------------------------------------------------------------------------------------------

    private CompositeTypeSerializerSnapshotTest.NestedSerializer createNestedSerializer(
            CompositeTypeSerializerSnapshotTest.TargetCompatibility targetCompatibility)
            throws Exception {
        Constructor<CompositeTypeSerializerSnapshotTest.NestedSerializer>
                nestedSerializerConstructor =
                        CompositeTypeSerializerSnapshotTest.NestedSerializer.class
                                .getDeclaredConstructor(
                                        CompositeTypeSerializerSnapshotTest.TargetCompatibility
                                                .class);
        nestedSerializerConstructor.setAccessible(true);
        return nestedSerializerConstructor.newInstance(targetCompatibility);
    }
}
