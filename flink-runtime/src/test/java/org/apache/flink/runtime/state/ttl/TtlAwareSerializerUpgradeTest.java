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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Ttl state migration test for {@link TtlAwareSerializer}. */
public class TtlAwareSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<String, TtlValue<String>> {

    private static final String TEST_DATA = "hello Gordon";

    @Override
    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion currentVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "ttl-aware-serializer-ttl-value",
                        currentVersion,
                        TtlAwareSerializerEnablingTtlSetup.class,
                        TtlAwareSerializerDisablingTtlVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "ttl-aware-serializer-string-value",
                        currentVersion,
                        TtlAwareSerializerDisablingTtlSetup.class,
                        TtlAwareSerializerEnablingTtlVerifier.class));
        return testSpecifications;
    }

    public static final class TtlAwareSerializerDisablingTtlSetup
            implements PreUpgradeSetup<String> {

        @Override
        public TypeSerializer<String> createPriorSerializer() {
            return new TtlAwareSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public String createTestData() {
            return TEST_DATA;
        }
    }

    public static final class TtlAwareSerializerEnablingTtlSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<TtlValue<String>> {

        @Override
        public TypeSerializer<TtlValue<String>> createPriorSerializer() {
            return new TtlAwareSerializer<>(
                    new TtlStateFactory.TtlSerializer<>(
                            LongSerializer.INSTANCE, StringSerializer.INSTANCE));
        }

        @Override
        public TtlValue<String> createTestData() {
            return new TtlValue<>(TEST_DATA, 13);
        }
    }

    public static final class TtlAwareSerializerDisablingTtlVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<String> {

        @Override
        public TypeSerializer<String> createUpgradedSerializer() {
            return new TtlAwareSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public Condition<String> testDataCondition() {
            return new Condition<>(value -> Objects.equals(value, TEST_DATA), "value");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<String>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAfterMigration();
        }
    }

    public static final class TtlAwareSerializerEnablingTtlVerifier
            implements UpgradeVerifier<TtlValue<String>> {

        @Override
        public TypeSerializer<TtlValue<String>> createUpgradedSerializer() {
            return new TtlAwareSerializer<>(
                    new TtlStateFactory.TtlSerializer<>(
                            LongSerializer.INSTANCE, StringSerializer.INSTANCE));
        }

        @Override
        public Condition<TtlValue<String>> testDataCondition() {
            return new Condition<>(
                    ttlValue -> Objects.equals(ttlValue.getUserValue(), TEST_DATA), "value");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<TtlValue<String>>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAfterMigration();
        }
    }

    @Disabled
    @ParameterizedTest(name = "Test Specification = {0}")
    @MethodSource("createTestSpecificationsForAllVersions")
    void restoreSerializerIsValid(TestSpecification<String, TtlValue<String>> testSpecification)
            throws Exception {
        // Restore test should be skipped for TtlMigrationTest, since the serializer value type will
        // be changed during ttl migration.
    }

    @Override
    public <T> DataInputView readAndThenWriteData(
            DataInputView originalDataInput,
            TypeSerializer<T> readSerializer,
            TypeSerializer<T> writeSerializer,
            Condition<T> testDataCondition)
            throws IOException {
        TtlAwareSerializer<T> reader = (TtlAwareSerializer<T>) readSerializer;
        TtlAwareSerializer<T> writer = (TtlAwareSerializer<T>) writeSerializer;

        DataOutputSerializer migratedOut = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
        writer.migrateValueFromPriorSerializer(
                reader,
                () -> reader.deserialize(originalDataInput),
                migratedOut,
                TtlTimeProvider.DEFAULT);
        DataInputView inputView = new DataInputDeserializer(migratedOut.wrapAsByteBuffer());

        T data = writer.deserialize(inputView);
        assertThat(data).is(testDataCondition);

        DataOutputSerializer out = new DataOutputSerializer(INITIAL_OUTPUT_BUFFER_SIZE);
        writeSerializer.serialize(data, out);
        return new DataInputDeserializer(out.wrapAsByteBuffer());
    }
}
