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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link LinkedListSerializer}. */
@RunWith(Parameterized.class)
public class LinkedListSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<LinkedList<Long>, LinkedList<Long>> {

    public LinkedListSerializerUpgradeTest(
            TestSpecification<LinkedList<Long>, LinkedList<Long>> testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {
        return Collections.singletonList(
                new TestSpecification<>(
                        "linked-list-serializer",
                        MigrationVersion.v1_13,
                        LinkedListSerializerSetup.class,
                        LinkedListSerializerVerifier.class));
    }

    public static TypeSerializer<LinkedList<Long>> createLinkedListSerializer() {
        return new LinkedListSerializer<>(new LongSerializer());
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "linked-row-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class LinkedListSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<LinkedList<Long>> {

        @Override
        public TypeSerializer<LinkedList<Long>> createPriorSerializer() {
            return createLinkedListSerializer();
        }

        @Override
        public LinkedList<Long> createTestData() {
            LinkedList<Long> list = new LinkedList<>();
            list.add(42L);
            list.add(-42L);
            list.add(0L);
            list.add(Long.MAX_VALUE);
            list.add(Long.MIN_VALUE);
            return list;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class LinkedListSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<LinkedList<Long>> {

        @Override
        public TypeSerializer<LinkedList<Long>> createUpgradedSerializer() {
            return createLinkedListSerializer();
        }

        @Override
        public Matcher<LinkedList<Long>> testDataMatcher() {
            LinkedList<Long> list = new LinkedList<>();
            list.add(42L);
            list.add(-42L);
            list.add(0L);
            list.add(Long.MAX_VALUE);
            list.add(Long.MIN_VALUE);
            return is(list);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<LinkedList<Long>>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            if (version.isNewerVersionThan(MigrationVersion.v1_13)) {
                return TypeSerializerMatchers.isCompatibleAsIs();
            } else {
                return TypeSerializerMatchers.isCompatibleAfterMigration();
            }
        }
    }
}
