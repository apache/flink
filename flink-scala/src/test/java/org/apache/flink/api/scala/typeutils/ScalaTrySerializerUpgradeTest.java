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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import scala.util.Failure;
import scala.util.Try;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link TrySerializer}. */
class ScalaTrySerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<Try<String>, Try<String>> {

    private static final String SPEC_NAME = "scala-try-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        ScalaTrySerializerSetup.class,
                        ScalaTrySerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "scala-try-serializer"
    // ----------------------------------------------------------------------------------------------
    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ScalaTrySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Try<String>> {
        @Override
        public TypeSerializer<Try<String>> createPriorSerializer() {
            return new TrySerializer<>(StringSerializer.INSTANCE, new ExecutionConfig());
        }

        @SuppressWarnings("unchecked")
        @Override
        public Try<String> createTestData() {
            return new Failure(new SpecifiedException("Specified exception for ScalaTry."));
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ScalaTrySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Try<String>> {
        @Override
        public TypeSerializer<Try<String>> createUpgradedSerializer() {
            return new TrySerializer<>(StringSerializer.INSTANCE, new ExecutionConfig());
        }

        @SuppressWarnings("unchecked")
        @Override
        public Matcher<Try<String>> testDataMatcher() {
            return is(new Failure(new SpecifiedException("Specified exception for ScalaTry.")));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Try<String>>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    /** A specified runtime exception override {@link #equals(Object)}. */
    public static final class SpecifiedException extends RuntimeException {
        public SpecifiedException(String message) {
            super(message);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof SpecifiedException)) {
                return false;
            }
            SpecifiedException other = (SpecifiedException) obj;
            return Objects.equals(getMessage(), other.getMessage());
        }
    }
}
