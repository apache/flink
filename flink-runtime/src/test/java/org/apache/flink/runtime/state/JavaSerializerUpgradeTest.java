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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link JavaSerializer}. */
@RunWith(Parameterized.class)
public class JavaSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<Serializable, Serializable> {

    private static final String SPEC_NAME = "java-serializer";

    public JavaSerializerUpgradeTest(
            TestSpecification<Serializable, Serializable> testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();

        for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            SPEC_NAME,
                            migrationVersion,
                            JavaSerializerSetup.class,
                            JavaSerializerVerifier.class));
        }

        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "java-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class JavaSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Serializable> {
        @Override
        public TypeSerializer<Serializable> createPriorSerializer() {
            return new JavaSerializer<>();
        }

        @Override
        public Serializable createTestData() {
            return 26;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class JavaSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Serializable> {
        @Override
        public TypeSerializer<Serializable> createUpgradedSerializer() {
            return new JavaSerializer<>();
        }

        @Override
        public Matcher<Serializable> testDataMatcher() {
            return is(26);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Serializable>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
