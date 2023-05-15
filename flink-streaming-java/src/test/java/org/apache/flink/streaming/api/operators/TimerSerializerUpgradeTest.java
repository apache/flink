/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

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

import static org.hamcrest.Matchers.is;

/** Migration test for {@link TimerSerializer}. */
class TimerSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<
                TimerHeapInternalTimer<String, Integer>, TimerHeapInternalTimer<String, Integer>> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "timer-serializer",
                        flinkVersion,
                        TimerSerializerSetup.class,
                        TimerSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "TimerSerializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TimerSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<
                    TimerHeapInternalTimer<String, Integer>> {
        @Override
        public TypeSerializer<TimerHeapInternalTimer<String, Integer>> createPriorSerializer() {
            return new TimerSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE);
        }

        @Override
        public TimerHeapInternalTimer<String, Integer> createTestData() {
            return new TimerHeapInternalTimer<>(12345, "key", 678);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TimerSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<
                    TimerHeapInternalTimer<String, Integer>> {
        @Override
        public TypeSerializer<TimerHeapInternalTimer<String, Integer>> createUpgradedSerializer() {
            return new TimerSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE);
        }

        @Override
        public Matcher<TimerHeapInternalTimer<String, Integer>> testDataMatcher() {
            return is(new TimerHeapInternalTimer<>(12345, "key", 678));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<TimerHeapInternalTimer<String, Integer>>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
