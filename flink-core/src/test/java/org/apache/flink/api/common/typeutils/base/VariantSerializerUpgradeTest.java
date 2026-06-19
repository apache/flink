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
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.VariantSerializer.VariantSerializerSnapshot;
import org.apache.flink.test.util.MigrationTest;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Disabled;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link VariantSerializerSnapshot}. The test is
 * disabled because Variant is introduced in Flink 2.1. We should restore the test when there is a
 * Flink 2.2 which should test compatibility with Flink 2.1
 */
@Disabled("FLINK-37951")
class VariantSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Variant, Variant> {

    private static final String SPEC_NAME = "variant-serializer";

    @Override
    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion currentVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        currentVersion,
                        VariantSerializerSetup.class,
                        VariantSerializerVerifier.class));

        return testSpecifications;
    }

    @Override
    public Collection<FlinkVersion> getMigrationVersions() {
        return FlinkVersion.rangeOf(
                FlinkVersion.v2_1, MigrationTest.getMostRecentlyPublishedVersion());
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "variant-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class VariantSerializerSetup implements PreUpgradeSetup<Variant> {
        @Override
        public TypeSerializer<Variant> createPriorSerializer() {
            return VariantSerializer.INSTANCE;
        }

        @Override
        public Variant createTestData() {
            VariantBuilder builder = Variant.newBuilder();
            return builder.object()
                    .add("k", builder.of(1))
                    .add("object", builder.object().add("k", builder.of("hello")).build())
                    .add(
                            "array",
                            builder.array()
                                    .add(builder.of(1))
                                    .add(builder.of(2))
                                    .add(builder.object().add("kk", builder.of(1.123f)).build())
                                    .build())
                    .build();
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class VariantSerializerVerifier implements UpgradeVerifier<Variant> {
        @Override
        public TypeSerializer<Variant> createUpgradedSerializer() {
            return VariantSerializer.INSTANCE;
        }

        @Override
        public Condition<Variant> testDataCondition() {
            VariantBuilder builder = Variant.newBuilder();
            Variant data =
                    builder.object()
                            .add("k", builder.of(1))
                            .add("object", builder.object().add("k", builder.of("hello")).build())
                            .add(
                                    "array",
                                    builder.array()
                                            .add(builder.of(1))
                                            .add(builder.of(2))
                                            .add(
                                                    builder.object()
                                                            .add("kk", builder.of(1.123f))
                                                            .build())
                                            .build())
                            .build();
            return new Condition<>(data::equals, "value is " + data);
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Variant>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
