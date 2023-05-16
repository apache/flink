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
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.api.common.typeutils.base.TestEnum.EMMA;
import static org.hamcrest.Matchers.is;

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

    private static Matcher<? extends TypeSerializer<TestEnum>> enumSerializerWith(
            final TestEnum[] expectedEnumValues) {
        return new TypeSafeMatcher<EnumSerializer<TestEnum>>() {

            @Override
            protected boolean matchesSafely(EnumSerializer<TestEnum> reconfiguredSerialized) {
                return Arrays.equals(reconfiguredSerialized.getValues(), expectedEnumValues);
            }

            @Override
            public void describeTo(Description description) {
                description
                        .appendText("EnumSerializer with values ")
                        .appendValueList("{", ", ", "}", expectedEnumValues);
            }
        };
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
            return new EnumSerializer(TestEnum.class);
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
            return new EnumSerializer(TestEnum.class);
        }

        @Override
        public Matcher<TestEnum> testDataMatcher() {
            return is(EMMA);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<TestEnum>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
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
            return new EnumSerializer(EnumBefore.class);
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
            return new EnumSerializer(EnumAfter.class);
        }

        @Override
        public Matcher<EnumAfter> testDataMatcher() {
            return is(EnumAfter.EMMA);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<EnumAfter>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer();
        }
    }
}
