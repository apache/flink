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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link NullableSerializer}. */
@RunWith(Parameterized.class)
public class NullableSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Long, Long> {

    public NullableSerializerUpgradeTest(TestSpecification<Long, Long> testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            "nullable-padded-serializer",
                            migrationVersion,
                            NullablePaddedSerializerSetup.class,
                            NullablePaddedSerializerVerifier.class));

            testSpecifications.add(
                    new TestSpecification<>(
                            "nullable-not-padded-serializer",
                            migrationVersion,
                            NullableNotPaddedSerializerSetup.class,
                            NullableNotPaddedSerializerVerifier.class));
        }
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
        public Matcher<Long> testDataMatcher() {
            return is((Long) null);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Long>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
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
        public Matcher<Long> testDataMatcher() {
            return is((Long) null);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Long>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
