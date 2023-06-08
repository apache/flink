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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;

import scala.util.Either;
import scala.util.Right;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link ScalaEitherSerializerSnapshot}. */
class ScalaEitherSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<Either<Integer, String>, Either<Integer, String>> {

    private static final String SPEC_NAME = "scala-either-serializer";

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        EitherSerializerSetup.class,
                        EitherSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "either-serializer-left"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class EitherSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Either<Integer, String>> {
        @Override
        public TypeSerializer<Either<Integer, String>> createPriorSerializer() {
            return new EitherSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        }

        @Override
        public Either<Integer, String> createTestData() {
            return new Right<>("Hello world");
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class EitherSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Either<Integer, String>> {
        @Override
        public TypeSerializer<Either<Integer, String>> createUpgradedSerializer() {
            return new EitherSerializer<>(IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        }

        @Override
        public Matcher<Either<Integer, String>> testDataMatcher() {
            return is(new Right<>("Hello world"));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<Either<Integer, String>>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
