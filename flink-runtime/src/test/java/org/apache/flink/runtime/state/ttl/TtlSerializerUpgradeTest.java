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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.runtime.state.ttl.TtlValueMatchers.ttlValue;
import static org.hamcrest.Matchers.is;

/** State migration test for {@link TtlSerializer}. */
@RunWith(Parameterized.class)
public class TtlSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<TtlValue<String>, TtlValue<String>> {

    public TtlSerializerUpgradeTest(
            TestSpecification<TtlValue<String>, TtlValue<String>> testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            "ttl-serializer",
                            migrationVersion,
                            TtlSerializerSetup.class,
                            TtlSerializerVerifier.class));
        }

        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "ttl-serializer"
    // ----------------------------------------------------------------------------------------------

    public static final class TtlSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<TtlValue<String>> {

        @Override
        public TypeSerializer<TtlValue<String>> createPriorSerializer() {
            return new TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
        }

        @Override
        public TtlValue<String> createTestData() {
            return new TtlValue<>("hello Gordon", 13);
        }
    }

    public static final class TtlSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<TtlValue<String>> {

        @Override
        public TypeSerializer<TtlValue<String>> createUpgradedSerializer() {
            return new TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
        }

        @Override
        public Matcher<TtlValue<String>> testDataMatcher() {
            return ttlValue(is("hello Gordon"), is(13L));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<TtlValue<String>>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
