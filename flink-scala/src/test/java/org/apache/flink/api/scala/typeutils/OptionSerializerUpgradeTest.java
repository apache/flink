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

package org.apache.flink.api.scala.typeutils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;

import scala.Option;

import static org.hamcrest.CoreMatchers.is;

/**
 * A {@link org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase} for {@link
 * ScalaEitherSerializerSnapshot}.
 */
class OptionSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<Option<String>, Option<String>> {

    private static final String SPEC_NAME = "scala-option-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        ScalaOptionSerializerSetup.class,
                        ScalaOptionSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "scala-option-serializer"
    // ----------------------------------------------------------------------------------------------
    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ScalaOptionSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Option<String>> {

        @Override
        public TypeSerializer<Option<String>> createPriorSerializer() {
            return new OptionSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public Option<String> createTestData() {
            return Option.empty();
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ScalaOptionSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Option<String>> {
        @Override
        public TypeSerializer<Option<String>> createUpgradedSerializer() {
            return new OptionSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public Matcher<Option<String>> testDataMatcher() {
            return is(Option.empty());
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Option<String>>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
