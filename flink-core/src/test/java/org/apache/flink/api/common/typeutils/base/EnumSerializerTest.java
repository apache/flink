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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnumSerializerTest {

    @Test
    void testPublicEnum() {
        testEnumSerializer(PrivateEnum.ONE, PrivateEnum.TWO, PrivateEnum.THREE);
    }

    @Test
    void testPrivateEnum() {
        testEnumSerializer(
                PublicEnum.FOO,
                PublicEnum.BAR,
                PublicEnum.PETER,
                PublicEnum.NATHANIEL,
                PublicEnum.EMMA,
                PublicEnum.PAULA);
    }

    @Test
    void testEmptyEnum() {
        assertThatThrownBy(() -> new EnumSerializer<>(EmptyEnum.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReconfiguration() {
        // mock the previous ordering of enum constants to be BAR, PAULA, NATHANIEL
        PublicEnum[] mockPreviousOrder = {PublicEnum.BAR, PublicEnum.PAULA, PublicEnum.NATHANIEL};

        // now, the actual order of FOO, BAR, PETER, NATHANIEL, EMMA, PAULA will be the "new wrong
        // order"
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify that the serializer is first using the "wrong order" (i.e., the initial new
        // configuration)
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());

        // reconfigure and verify compatibility
        EnumSerializer.EnumSerializerSnapshot serializerSnapshot =
                new EnumSerializer.EnumSerializerSnapshot(PublicEnum.class, mockPreviousOrder);
        TypeSerializerSchemaCompatibility compatibility =
                serializer.snapshotConfiguration().resolveSchemaCompatibility(serializerSnapshot);
        assertThat(compatibility.isCompatibleWithReconfiguredSerializer()).isTrue();

        // after reconfiguration, the order should be first the original BAR, PAULA, NATHANIEL,
        // followed by the "new enum constants" FOO, PETER, EMMA
        PublicEnum[] expectedOrder = {
            PublicEnum.BAR,
            PublicEnum.PAULA,
            PublicEnum.NATHANIEL,
            PublicEnum.FOO,
            PublicEnum.PETER,
            PublicEnum.EMMA
        };

        EnumSerializer<PublicEnum> configuredSerializer =
                (EnumSerializer<PublicEnum>) compatibility.getReconfiguredSerializer();
        int i = 0;
        for (PublicEnum constant : expectedOrder) {
            assertThat(configuredSerializer.getValueToOrdinal().get(constant).intValue())
                    .isEqualTo(i);
            i++;
        }

        assertThat(configuredSerializer.getValues()).isEqualTo(expectedOrder);
    }

    @Test
    void testConfigurationSnapshotSerialization() throws Exception {
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), serializer.snapshotConfiguration());
            serializedConfig = out.toByteArray();
        }

        TypeSerializerSnapshot<PublicEnum> restoredConfig;
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            restoredConfig =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        TypeSerializerSchemaCompatibility<PublicEnum> compatResult =
                serializer.snapshotConfiguration().resolveSchemaCompatibility(restoredConfig);
        assertThat(compatResult.isCompatibleAsIs()).isTrue();

        assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());
        assertThat(serializer.getValues()).isEqualTo(PublicEnum.values());
    }

    @Test
    void testSerializeEnumSerializer() throws Exception {
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify original transient parameters
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());
        assertThat(serializer.getValues()).isEqualTo(PublicEnum.values());

        byte[] serializedSerializer = InstantiationUtil.serializeObject(serializer);

        // deserialize and re-verify transient parameters
        serializer =
                InstantiationUtil.deserializeObject(
                        serializedSerializer, Thread.currentThread().getContextClassLoader());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());
        assertThat(serializer.getValues()).isEqualTo(PublicEnum.values());
    }

    @Test
    void testSerializeReconfiguredEnumSerializer() {
        // mock the previous ordering of enum constants to be BAR, PAULA, NATHANIEL
        PublicEnum[] mockPreviousOrder = {PublicEnum.BAR, PublicEnum.PAULA, PublicEnum.NATHANIEL};

        // now, the actual order of FOO, BAR, PETER, NATHANIEL, EMMA, PAULA will be the "new wrong
        // order"
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify that the serializer is first using the "wrong order" (i.e., the initial new
        // configuration)
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue())
                .isEqualTo(PublicEnum.FOO.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue())
                .isEqualTo(PublicEnum.BAR.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue())
                .isEqualTo(PublicEnum.PETER.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue())
                .isEqualTo(PublicEnum.NATHANIEL.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue())
                .isEqualTo(PublicEnum.EMMA.ordinal());
        assertThat(serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue())
                .isEqualTo(PublicEnum.PAULA.ordinal());

        // reconfigure and verify compatibility
        EnumSerializer.EnumSerializerSnapshot serializerSnapshot =
                new EnumSerializer.EnumSerializerSnapshot(PublicEnum.class, mockPreviousOrder);
        TypeSerializerSchemaCompatibility compatibility =
                serializer.snapshotConfiguration().resolveSchemaCompatibility(serializerSnapshot);
        assertThat(compatibility.isCompatibleWithReconfiguredSerializer()).isTrue();

        // verify that after the serializer was read, the reconfigured constant ordering is
        // untouched
        PublicEnum[] expectedOrder = {
            PublicEnum.BAR,
            PublicEnum.PAULA,
            PublicEnum.NATHANIEL,
            PublicEnum.FOO,
            PublicEnum.PETER,
            PublicEnum.EMMA
        };

        EnumSerializer<PublicEnum> configuredSerializer =
                (EnumSerializer<PublicEnum>) compatibility.getReconfiguredSerializer();
        int i = 0;
        for (PublicEnum constant : expectedOrder) {
            assertThat(configuredSerializer.getValueToOrdinal().get(constant).intValue())
                    .isEqualTo(i);
            i++;
        }

        assertThat(configuredSerializer.getValues()).isEqualTo(expectedOrder);
    }

    @SafeVarargs
    public final <T extends Enum<T>> void testEnumSerializer(T... data) {
        @SuppressWarnings("unchecked")
        final Class<T> clazz = (Class<T>) data.getClass().getComponentType();

        SerializerTestInstance<T> tester =
                new SerializerTestInstance<T>(new EnumSerializer<>(clazz), clazz, 4, data) {};

        tester.testAll();
    }

    // ------------------------------------------------------------------------
    //  Test enums
    // ------------------------------------------------------------------------

    public enum PublicEnum {
        FOO,
        BAR,
        PETER,
        NATHANIEL,
        EMMA,
        PAULA
    }

    public enum EmptyEnum {}

    private enum PrivateEnum {
        ONE,
        TWO,
        THREE
    }
}
