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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.runtime.CopyableSerializerUpgradeTest.SimpleCopyable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

/** A {@link TypeSerializerUpgradeTestBase} for {@link CopyableValueSerializer}. */
class CopyableSerializerUpgradeTest
        extends TypeSerializerUpgradeTestBase<SimpleCopyable, SimpleCopyable> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion flinkVersion)
            throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        testSpecifications.add(
                new TestSpecification<>(
                        "copyable-value-serializer",
                        flinkVersion,
                        CopyableSerializerSetup.class,
                        CopyableSerializerVerifier.class));
        return testSpecifications;
    }

    /** A simple copyable value for migration tests. */
    @SuppressWarnings("WeakerAccess")
    public static final class SimpleCopyable implements CopyableValue<SimpleCopyable> {

        public static final long serialVersionUID = 1;

        private long value;

        public SimpleCopyable() {}

        public SimpleCopyable(long value) {
            this.value = value;
        }

        @Override
        public int getBinaryLength() {
            return 8;
        }

        @Override
        public void copyTo(SimpleCopyable target) {
            target.value = this.value;
        }

        @Override
        public SimpleCopyable copy() {
            return new SimpleCopyable(value);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeLong(value);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            value = in.readLong();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (!(obj instanceof SimpleCopyable)) {
                return false;
            }

            SimpleCopyable other = (SimpleCopyable) obj;
            return value == other.value;
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "copyable-value-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class CopyableSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<SimpleCopyable> {
        @Override
        public TypeSerializer<SimpleCopyable> createPriorSerializer() {
            return new CopyableValueSerializer<>(SimpleCopyable.class);
        }

        @Override
        public SimpleCopyable createTestData() {
            return new SimpleCopyable(123456);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class CopyableSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<SimpleCopyable> {
        @Override
        public TypeSerializer<SimpleCopyable> createUpgradedSerializer() {
            return new CopyableValueSerializer<>(SimpleCopyable.class);
        }

        @Override
        public Matcher<SimpleCopyable> testDataMatcher() {
            return is(new SimpleCopyable(123456));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<SimpleCopyable>>
                schemaCompatibilityMatcher(FlinkVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    @Test
    void testSimpleCopyableEqualsImplementation() {
        SimpleCopyable a = new SimpleCopyable(123456);
        assertThat(a).isEqualTo(new SimpleCopyable(123456));
    }
}
