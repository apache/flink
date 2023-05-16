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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link ListSerializerSnapshot}. */
class ListSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<List<String>, List<String>> {

    private static final String SPEC_NAME = "list-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        ListSerializerSetup.class,
                        ListSerializerVerifier.class));

        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "list-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ListSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<List<String>> {
        @Override
        public TypeSerializer<List<String>> createPriorSerializer() {
            return new ListSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public List<String> createTestData() {
            List<String> data = new ArrayList<>(2);
            data.add("Apache");
            data.add("Flink");
            return data;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ListSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<List<String>> {
        @Override
        public TypeSerializer<List<String>> createUpgradedSerializer() {
            return new ListSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public Matcher<List<String>> testDataMatcher() {
            List<String> data = new ArrayList<>(2);
            data.add("Apache");
            data.add("Flink");
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<List<String>>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
