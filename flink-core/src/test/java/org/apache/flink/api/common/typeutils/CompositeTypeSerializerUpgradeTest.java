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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.types.Either;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/** A {@link TypeSerializerUpgradeTestBase} for {@link GenericArraySerializer}. */
class CompositeTypeSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "either-serializer",
                        flinkVersion,
                        EitherSerializerSetup.class,
                        EitherSerializerVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "generic-array-serializer",
                        flinkVersion,
                        GenericArraySerializerSetup.class,
                        GenericArraySerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "either-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class EitherSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Either<String, Integer>> {
        @Override
        public TypeSerializer<Either<String, Integer>> createPriorSerializer() {
            return new EitherSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE);
        }

        @Override
        public Either<String, Integer> createTestData() {
            return new Either.Left<>("ApacheFlink");
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class EitherSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Either<String, Integer>> {
        @Override
        public TypeSerializer<Either<String, Integer>> createUpgradedSerializer() {
            return new EitherSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE);
        }

        @Override
        public Condition<Either<String, Integer>> testDataCondition() {
            return new Condition<>(
                    value -> new Either.Left<>("ApacheFlink").equals(value),
                    "value is Either.Left(\"ApacheFlink\")");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Either<String, Integer>>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "generic-array-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class GenericArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<String[]> {
        @Override
        public TypeSerializer<String[]> createPriorSerializer() {
            return new GenericArraySerializer<>(String.class, StringSerializer.INSTANCE);
        }

        @Override
        public String[] createTestData() {
            return new String[] {"Apache", "Flink"};
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class GenericArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<String[]> {
        @Override
        public TypeSerializer<String[]> createUpgradedSerializer() {
            return new GenericArraySerializer<>(String.class, StringSerializer.INSTANCE);
        }

        @Override
        public Condition<String[]> testDataCondition() {
            String[] data = {"Apache", "Flink"};
            return new Condition<>(
                    value -> Arrays.equals(data, value), "data is " + Arrays.toString(data));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<String[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    @Test
    void testUpgradeFromDeprecatedSnapshot() {
        GenericArraySerializer<String> genericArraySerializer =
                new GenericArraySerializer<>(String.class, StringSerializer.INSTANCE);
        GenericArraySerializerConfigSnapshot<String> oldSnapshot =
                new GenericArraySerializerConfigSnapshot<>(genericArraySerializer);
        TypeSerializerSchemaCompatibility<String[]> schemaCompatibility =
                genericArraySerializer
                        .snapshotConfiguration()
                        .resolveSchemaCompatibility(oldSnapshot);
        assertThat(schemaCompatibility.isCompatibleAsIs()).isTrue();
    }
}
