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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.ClassRelocator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;

import org.assertj.core.api.Condition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.api.common.typeutils.base.TestEnum.EMMA;

/** Migration tests for {@link EnumSerializer}. */
class EnumSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<TestEnum, TestEnum> {
    private static final String SPEC_NAME = "enum-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        EnumSerializerSetup.class,
                        EnumSerializerVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME + "reconfig",
                        flinkVersion,
                        EnumSerializerReconfigSetup.class,
                        EnumSerializerReconfigVerifier.class));
        return testSpecifications;
    }

    private static Condition<? extends TypeSerializer<TestEnum>> enumSerializerWith(
            final TestEnum[] expectedEnumValues) {
        return new Condition<EnumSerializer<TestEnum>>(
                value -> Arrays.equals(value.getValues(), expectedEnumValues), "");
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "enum-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class EnumSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<TestEnum> {
        @SuppressWarnings("unchecked")
        @Override
        public TypeSerializer<TestEnum> createPriorSerializer() {
            return new EnumSerializer<>(TestEnum.class);
        }

        @Override
        public TestEnum createTestData() {
            return EMMA;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class EnumSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<TestEnum> {
        @SuppressWarnings("unchecked")
        @Override
        public TypeSerializer<TestEnum> createUpgradedSerializer() {
            return new EnumSerializer<>(TestEnum.class);
        }

        @Override
        public Condition<TestEnum> testDataCondition() {
            return new Condition<>(value -> value == EMMA, "is EMMA");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<TestEnum>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class EnumSerializerReconfigSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<
                    EnumSerializerReconfigSetup.EnumBefore> {
        @ClassRelocator.RelocateClass("TestEnumSerializerReconfig")
        public enum EnumBefore {
            FOO,
            BAR,
            PETER,
            NATHANIEL,
            EMMA,
            PAULA
        }

        @SuppressWarnings("unchecked")
        @Override
        public TypeSerializer<EnumBefore> createPriorSerializer() {
            return new EnumSerializer<>(EnumBefore.class);
        }

        @Override
        public EnumBefore createTestData() {
            return EnumBefore.EMMA;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class EnumSerializerReconfigVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<
                    EnumSerializerReconfigVerifier.EnumAfter> {
        @ClassRelocator.RelocateClass("TestEnumSerializerReconfig")
        public enum EnumAfter {
            FOO,
            BAR,
            PETER,
            PAULA,
            NATHANIEL,
            EMMA
        }

        @SuppressWarnings("unchecked")
        @Override
        public TypeSerializer<EnumAfter> createUpgradedSerializer() {
            return new EnumSerializer<>(EnumAfter.class);
        }

        @Override
        public Condition<EnumAfter> testDataCondition() {
            return new Condition<>(value -> value == EnumAfter.EMMA, "is EMMA");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<EnumAfter>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleWithReconfiguredSerializer();
        }
    }
}
