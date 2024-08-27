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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.assertj.core.api.Condition;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link TimeWindow.Serializer} and {@link
 * GlobalWindow.Serializer}.
 */
class WindowSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "time-window-serializer",
                        flinkVersion,
                        TimeWindowSerializerSetup.class,
                        TimeWindowSerializerVerifier.class));
        testSpecifications.add(
                new TestSpecification<>(
                        "global-window-serializer",
                        flinkVersion,
                        GlobalWindowSerializerSetup.class,
                        GlobalWindowSerializerVerifier.class));
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "time-window-serializer"
    // ----------------------------------------------------------------------------------------------
    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TimeWindowSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<TimeWindow> {
        @Override
        public TypeSerializer<TimeWindow> createPriorSerializer() {
            return new TimeWindow.Serializer();
        }

        @Override
        public TimeWindow createTestData() {
            return new TimeWindow(12345, 67890);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TimeWindowSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<TimeWindow> {
        @Override
        public TypeSerializer<TimeWindow> createUpgradedSerializer() {
            return new TimeWindow.Serializer();
        }

        @Override
        public Condition<TimeWindow> testDataCondition() {
            return new Condition<>(
                    timeWindow -> new TimeWindow(12345, 67890).equals(timeWindow), "");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<TimeWindow>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "global-window-serializer"
    // ----------------------------------------------------------------------------------------------
    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class GlobalWindowSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<GlobalWindow> {
        @Override
        public TypeSerializer<GlobalWindow> createPriorSerializer() {
            return new GlobalWindow.Serializer();
        }

        @Override
        public GlobalWindow createTestData() {
            return GlobalWindow.get();
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class GlobalWindowSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<GlobalWindow> {
        @Override
        public TypeSerializer<GlobalWindow> createUpgradedSerializer() {
            return new GlobalWindow.Serializer();
        }

        @Override
        public Condition<GlobalWindow> testDataCondition() {
            return new Condition<>(GlobalWindow.get()::equals, "");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<GlobalWindow>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
