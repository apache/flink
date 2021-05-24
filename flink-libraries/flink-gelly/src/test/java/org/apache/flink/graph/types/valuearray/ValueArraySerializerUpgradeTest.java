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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/** Migration tests for boxed-value array serializer snapshots. */
@RunWith(Parameterized.class)
public class ValueArraySerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public ValueArraySerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
            testSpecifications.add(
                    new TestSpecification<>(
                            "byte-value-array-serializer",
                            migrationVersion,
                            ByteValueArraySerializerSetup.class,
                            ByteValueArraySerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "char-value-array-serializer",
                            migrationVersion,
                            CharValueArraySerializerSetup.class,
                            CharValueArraySerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "double-value-array-serializer",
                            migrationVersion,
                            DoubleValueArraySerializerSetup.class,
                            DoubleValueArraySerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "float-value-array-serializer",
                            migrationVersion,
                            FloatValueArraySerializerSetup.class,
                            FloatValueArraySerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "int-value-array-serializer",
                            migrationVersion,
                            IntValueArraySerializerSetup.class,
                            IntValueArraySerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "long-value-array-serializer",
                            migrationVersion,
                            LongValueArraySerializerSetup.class,
                            LongValueArraySerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "null-value-array-serializer",
                            migrationVersion,
                            NullValueArraySerializerSetup.class,
                            NullValueArraySerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "short-value-array-serializer",
                            migrationVersion,
                            ShortValueArraySerializerSetup.class,
                            ShortValueArraySerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "string-value-array-serializer",
                            migrationVersion,
                            StringValueArraySerializerSetup.class,
                            StringValueArraySerializerVerifier.class));
        }
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "byte-value-array-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ByteValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ByteValueArray> {
        @Override
        public TypeSerializer<ByteValueArray> createPriorSerializer() {
            return new ByteValueArraySerializer();
        }

        @Override
        public ByteValueArray createTestData() {
            ByteValueArray byteValues = new ByteValueArray(12345);
            byteValues.add(new ByteValue((byte) 123));
            byteValues.add(new ByteValue((byte) 345));
            return byteValues;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ByteValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ByteValueArray> {
        @Override
        public TypeSerializer<ByteValueArray> createUpgradedSerializer() {
            return new ByteValueArraySerializer();
        }

        @Override
        public Matcher<ByteValueArray> testDataMatcher() {
            ByteValueArray byteValues = new ByteValueArray(12345);
            byteValues.add(new ByteValue((byte) 123));
            byteValues.add(new ByteValue((byte) 345));
            return is(byteValues);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<ByteValueArray>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "char-value-array serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class CharValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<CharValueArray> {
        @Override
        public TypeSerializer<CharValueArray> createPriorSerializer() {
            return new CharValueArraySerializer();
        }

        @Override
        public CharValueArray createTestData() {
            CharValueArray array = new CharValueArray(128);
            array.add(new CharValue((char) 23));
            array.add(new CharValue((char) 34));
            array.add(new CharValue((char) 45));
            return array;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class CharValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<CharValueArray> {
        @Override
        public TypeSerializer<CharValueArray> createUpgradedSerializer() {
            return new CharValueArraySerializer();
        }

        @Override
        public Matcher<CharValueArray> testDataMatcher() {
            CharValueArray array = new CharValueArray(128);
            array.add(new CharValue((char) 23));
            array.add(new CharValue((char) 34));
            array.add(new CharValue((char) 45));
            return is(array);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<CharValueArray>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for ""double-value-array-serializer""
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class DoubleValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<DoubleValueArray> {
        @Override
        public TypeSerializer<DoubleValueArray> createPriorSerializer() {
            return new DoubleValueArraySerializer();
        }

        @Override
        public DoubleValueArray createTestData() {
            DoubleValueArray array = new DoubleValueArray(128);
            array.add(new DoubleValue(1.2));
            array.add(new DoubleValue(3.4));
            return array;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class DoubleValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<DoubleValueArray> {
        @Override
        public TypeSerializer<DoubleValueArray> createUpgradedSerializer() {
            return new DoubleValueArraySerializer();
        }

        @Override
        public Matcher<DoubleValueArray> testDataMatcher() {
            DoubleValueArray array = new DoubleValueArray(128);
            array.add(new DoubleValue(1.2));
            array.add(new DoubleValue(3.4));
            return is(array);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<DoubleValueArray>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for ""float-value-array-serializer""
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class FloatValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<FloatValueArray> {
        @Override
        public TypeSerializer<FloatValueArray> createPriorSerializer() {
            return new FloatValueArraySerializer();
        }

        @Override
        public FloatValueArray createTestData() {
            FloatValueArray array = new FloatValueArray(128);
            array.add(new FloatValue(1.2f));
            array.add(new FloatValue(3.4f));
            return array;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class FloatValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<FloatValueArray> {
        @Override
        public TypeSerializer<FloatValueArray> createUpgradedSerializer() {
            return new FloatValueArraySerializer();
        }

        @Override
        public Matcher<FloatValueArray> testDataMatcher() {
            FloatValueArray array = new FloatValueArray(128);
            array.add(new FloatValue(1.2f));
            array.add(new FloatValue(3.4f));
            return is(array);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<FloatValueArray>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "int-value-array-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class IntValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<IntValueArray> {
        @Override
        public TypeSerializer<IntValueArray> createPriorSerializer() {
            return new IntValueArraySerializer();
        }

        @Override
        public IntValueArray createTestData() {
            IntValueArray array = new IntValueArray(128);
            array.add(new IntValue(123));
            array.add(new IntValue(456));
            return array;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class IntValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<IntValueArray> {
        @Override
        public TypeSerializer<IntValueArray> createUpgradedSerializer() {
            return new IntValueArraySerializer();
        }

        @Override
        public Matcher<IntValueArray> testDataMatcher() {
            IntValueArray array = new IntValueArray(128);
            array.add(new IntValue(123));
            array.add(new IntValue(456));
            return is(array);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<IntValueArray>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "long-value-array-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class LongValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<LongValueArray> {
        @Override
        public TypeSerializer<LongValueArray> createPriorSerializer() {
            return new LongValueArraySerializer();
        }

        @Override
        public LongValueArray createTestData() {
            LongValueArray array = new LongValueArray(128);
            array.add(new LongValue(123L));
            array.add(new LongValue(456L));
            return array;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class LongValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<LongValueArray> {
        @Override
        public TypeSerializer<LongValueArray> createUpgradedSerializer() {
            return new LongValueArraySerializer();
        }

        @Override
        public Matcher<LongValueArray> testDataMatcher() {
            LongValueArray array = new LongValueArray(128);
            array.add(new LongValue(123L));
            array.add(new LongValue(456L));
            return is(array);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<LongValueArray>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "null-value-array-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class NullValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<NullValueArray> {
        @Override
        public TypeSerializer<NullValueArray> createPriorSerializer() {
            return new NullValueArraySerializer();
        }

        @Override
        public NullValueArray createTestData() {
            NullValueArray array = new NullValueArray(128);
            array.add(new NullValue());
            array.add(new NullValue());
            return array;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class NullValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<NullValueArray> {
        @Override
        public TypeSerializer<NullValueArray> createUpgradedSerializer() {
            return new NullValueArraySerializer();
        }

        @Override
        public Matcher<NullValueArray> testDataMatcher() {
            NullValueArray array = new NullValueArray(128);
            array.add(new NullValue());
            array.add(new NullValue());
            return is(array);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<NullValueArray>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for ""short-value-array-serializer""
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ShortValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ShortValueArray> {
        @Override
        public TypeSerializer<ShortValueArray> createPriorSerializer() {
            return new ShortValueArraySerializer();
        }

        @Override
        public ShortValueArray createTestData() {
            ShortValueArray array = new ShortValueArray(128);
            array.add(new ShortValue((short) 123));
            array.add(new ShortValue((short) 456));
            return array;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ShortValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ShortValueArray> {
        @Override
        public TypeSerializer<ShortValueArray> createUpgradedSerializer() {
            return new ShortValueArraySerializer();
        }

        @Override
        public Matcher<ShortValueArray> testDataMatcher() {
            ShortValueArray array = new ShortValueArray(128);
            array.add(new ShortValue((short) 123));
            array.add(new ShortValue((short) 456));
            return is(array);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<ShortValueArray>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for ""string-value-array-serializer""
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class StringValueArraySerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StringValueArray> {
        @Override
        public TypeSerializer<StringValueArray> createPriorSerializer() {
            return new StringValueArraySerializer();
        }

        @Override
        public StringValueArray createTestData() {
            StringValueArray array = new StringValueArray(128);
            array.add(new StringValue("123"));
            array.add(new StringValue("456"));
            return array;
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class StringValueArraySerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StringValueArray> {
        @Override
        public TypeSerializer<StringValueArray> createUpgradedSerializer() {
            return new StringValueArraySerializer();
        }

        @Override
        public Matcher<StringValueArray> testDataMatcher() {
            StringValueArray array = new StringValueArray(128);
            array.add(new StringValue("123"));
            array.add(new StringValue("456"));
            return is(array);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<StringValueArray>>
                schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
