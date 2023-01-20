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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link ArrayListSerializerSnapshot}. */
class ArrayListSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<ArrayList<String>, ArrayList<String>> {

    private static final String SPEC_NAME = "arraylist-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        ArrayListSerializerSetup.class,
                        ArrayListSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "arraylist-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ArrayListSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ArrayList<String>> {
        @Override
        public TypeSerializer<ArrayList<String>> createPriorSerializer() {
            return new ArrayListSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public ArrayList<String> createTestData() {
            ArrayList<String> data = new ArrayList<>(2);
            data.add("Apache");
            data.add("Flink");
            return data;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ArrayListSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ArrayList<String>> {
        @Override
        public TypeSerializer<ArrayList<String>> createUpgradedSerializer() {
            return new ArrayListSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public Matcher<ArrayList<String>> testDataMatcher() {
            ArrayList<String> data = new ArrayList<>(2);
            data.add("Apache");
            data.add("Flink");
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<ArrayList<String>>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
