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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.streaming.util.StreamRecordMatchers.streamRecord;
import static org.hamcrest.Matchers.is;

/** Migration tests for {@link StreamElementSerializer}. */
class StreamElementSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<StreamElement, StreamElement> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "stream-element-serializer",
                        flinkVersion,
                        StreamElementSetup.class,
                        StreamElementVerifier.class));

        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "StreamElement-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class StreamElementSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StreamElement> {
        @Override
        public TypeSerializer<StreamElement> createPriorSerializer() {
            return new StreamElementSerializer<>(StringSerializer.INSTANCE);
        }

        @Override
        public StreamElement createTestData() {
            return new StreamRecord<>("key", 123456);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class StreamElementVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StreamElement> {
        @Override
        public TypeSerializer<StreamElement> createUpgradedSerializer() {
            return new StreamElementSerializer<>(StringSerializer.INSTANCE);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Matcher<StreamElement> testDataMatcher() {
            return (Matcher) streamRecord(is("key"), is(123456L));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<StreamElement>> schemaCompatibilityMatcher(
                FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
