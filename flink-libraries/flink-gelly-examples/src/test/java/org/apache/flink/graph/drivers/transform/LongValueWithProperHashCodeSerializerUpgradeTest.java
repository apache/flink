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

package org.apache.flink.graph.drivers.transform;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link
 * LongValueWithProperHashCode.LongValueWithProperHashCodeSerializer}.
 */
@RunWith(Parameterized.class)
public class LongValueWithProperHashCodeSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<
                LongValueWithProperHashCode, LongValueWithProperHashCode> {

    public LongValueWithProperHashCodeSerializerUpgradeTest(
            TestSpecification<LongValueWithProperHashCode, LongValueWithProperHashCode>
                    testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            "long-value-with-proper-hash-code-serializer",
                            migrationVersion,
                            LongValueWithProperHashCodeSerializerSetup.class,
                            LongValueWithProperHashCodeSerializerVerifier.class));
        }
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "long-value-with-proper-hash-code-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class LongValueWithProperHashCodeSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<LongValueWithProperHashCode> {
        @Override
        public TypeSerializer<LongValueWithProperHashCode> createPriorSerializer() {
            return new LongValueWithProperHashCode.LongValueWithProperHashCodeSerializer();
        }

        @Override
        public LongValueWithProperHashCode createTestData() {
            return new LongValueWithProperHashCode(12345);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class LongValueWithProperHashCodeSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<LongValueWithProperHashCode> {
        @Override
        public TypeSerializer<LongValueWithProperHashCode> createUpgradedSerializer() {
            return new LongValueWithProperHashCode.LongValueWithProperHashCodeSerializer();
        }

        @Override
        public Matcher<LongValueWithProperHashCode> testDataMatcher() {
            return is(new LongValueWithProperHashCode(12345));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<LongValueWithProperHashCode>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
