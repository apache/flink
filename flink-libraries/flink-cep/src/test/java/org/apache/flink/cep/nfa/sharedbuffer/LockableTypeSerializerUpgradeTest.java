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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.test.util.MigrationTest;

import org.assertj.core.api.Condition;

import java.util.ArrayList;
import java.util.Collection;

/** A {@link TypeSerializerUpgradeTestBase} for {@link Lockable.LockableTypeSerializer}. */
class LockableTypeSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<Lockable<String>, Lockable<String>> {

    private static final String SPEC_NAME = "lockable-type-serializer";

    // we dropped support for old versions and only test against versions since 1.20.
    @Override
    public Collection<FlinkVersion> getMigrationVersions() {
        return FlinkVersion.rangeOf(
                FlinkVersion.v1_20, MigrationTest.getMostRecentlyPublishedVersion());
    }

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        SPEC_NAME,
                        flinkVersion,
                        LockableTypeSerializerSetup.class,
                        LockableTypeSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "lockabletype-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class LockableTypeSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Lockable<String>> {
        @Override
        public TypeSerializer<Lockable<String>> createPriorSerializer() {
            return new Lockable.LockableTypeSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public Lockable<String> createTestData() {
            return new Lockable<>("flink", 10);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class LockableTypeSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Lockable<String>> {
        @Override
        public TypeSerializer<Lockable<String>> createUpgradedSerializer() {
            return new Lockable.LockableTypeSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public Condition<Lockable<String>> testDataCondition() {
            return new Condition<>(value -> new Lockable<>("flink", 10).equals(value), "");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Lockable<String>>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
