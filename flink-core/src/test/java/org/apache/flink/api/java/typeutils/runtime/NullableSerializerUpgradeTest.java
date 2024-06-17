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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;

import org.assertj.core.api.Condition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/** A {@link TypeSerializerUpgradeTestBase} for {@link NullableSerializer}. */
class NullableSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Long, Long> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "nullable-padded-serializer",
                        flinkVersion,
                        NullablePaddedSerializerSetup.class,
                        NullablePaddedSerializerVerifier.class));

        testSpecifications.add(
                new TestSpecification<>(
                        "nullable-not-padded-serializer",
                        flinkVersion,
                        NullableNotPaddedSerializerSetup.class,
                        NullableNotPaddedSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "nullable-padded-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class NullablePaddedSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Long> {
        @Override
        public TypeSerializer<Long> createPriorSerializer() {
            return NullableSerializer.wrap(LongSerializer.INSTANCE, true);
        }

        @Override
        public Long createTestData() {
            return null;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class NullablePaddedSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Long> {
        @Override
        public TypeSerializer<Long> createUpgradedSerializer() {
            return NullableSerializer.wrap(LongSerializer.INSTANCE, true);
        }

        @Override
        public Condition<Long> testDataCondition() {
            return new Condition<>(Objects::isNull, "value is null");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Long>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "nullable-not-padded-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class NullableNotPaddedSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Long> {
        @Override
        public TypeSerializer<Long> createPriorSerializer() {
            return NullableSerializer.wrap(LongSerializer.INSTANCE, false);
        }

        @Override
        public Long createTestData() {
            return null;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class NullableNotPaddedSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Long> {
        @Override
        public TypeSerializer<Long> createUpgradedSerializer() {
            return NullableSerializer.wrap(LongSerializer.INSTANCE, false);
        }

        @Override
        public Condition<Long> testDataCondition() {
            return new Condition<>(Objects::isNull, "value is null");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Long>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
