/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertSame;

public class PrimitiveArraySerializerUpgradeTestSpecifications {
    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive boolean array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveBooleanArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<boolean[]> {
        @Override
        public TypeSerializer<boolean[]> createPriorSerializer() {
            TypeSerializer<boolean[]> serializer =
                    TypeExtractor.createTypeInfo(boolean[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), BooleanPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public boolean[] createTestData() {
            boolean[] data = new boolean[2];
            data[0] = true;
            data[1] = false;
            return data;
        }
    }

    public static final class PrimitiveBooleanArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<boolean[]> {
        @Override
        public TypeSerializer<boolean[]> createUpgradedSerializer() {
            TypeSerializer<boolean[]> serializer =
                    TypeExtractor.createTypeInfo(boolean[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), BooleanPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<boolean[]> testDataMatcher() {
            boolean[] expected = new boolean[2];
            expected[0] = true;
            expected[1] = false;
            return is(expected);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<boolean[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive byte array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveByteArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<byte[]> {
        @Override
        public TypeSerializer<byte[]> createPriorSerializer() {
            TypeSerializer<byte[]> serializer =
                    TypeExtractor.createTypeInfo(byte[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), BytePrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public byte[] createTestData() {
            byte[] dummy = new byte[10];
            for (int i = 0; i < dummy.length; ++i) {
                dummy[i] = (byte) i;
            }
            return dummy;
        }
    }

    public static final class PrimitiveByteArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<byte[]> {
        @Override
        public TypeSerializer<byte[]> createUpgradedSerializer() {
            TypeSerializer<byte[]> serializer =
                    TypeExtractor.createTypeInfo(byte[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), BytePrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<byte[]> testDataMatcher() {
            byte[] expected = new byte[10];
            for (int i = 0; i < expected.length; ++i) {
                expected[i] = (byte) i;
            }
            return is(expected);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<byte[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive char array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveCharArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<char[]> {
        @Override
        public TypeSerializer<char[]> createPriorSerializer() {
            TypeSerializer<char[]> serializer =
                    TypeExtractor.createTypeInfo(char[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), CharPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public char[] createTestData() {
            char[] data = new char[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = (char) i;
            }
            return data;
        }
    }

    public static final class PrimitiveCharArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<char[]> {
        @Override
        public TypeSerializer<char[]> createUpgradedSerializer() {
            TypeSerializer<char[]> serializer =
                    TypeExtractor.createTypeInfo(char[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), CharPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<char[]> testDataMatcher() {
            char[] data = new char[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = (char) i;
            }
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<char[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive double array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveDoubleArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<double[]> {
        @Override
        public TypeSerializer<double[]> createPriorSerializer() {
            TypeSerializer<double[]> serializer =
                    TypeExtractor.createTypeInfo(double[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), DoublePrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public double[] createTestData() {
            double[] data = new double[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i + 0.1f;
            }
            return data;
        }
    }

    public static final class PrimitiveDoubleArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<double[]> {
        @Override
        public TypeSerializer<double[]> createUpgradedSerializer() {
            TypeSerializer<double[]> serializer =
                    TypeExtractor.createTypeInfo(double[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), DoublePrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<double[]> testDataMatcher() {
            double[] data = new double[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i + 0.1f;
            }
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<double[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive float array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveFloatArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<float[]> {
        @Override
        public TypeSerializer<float[]> createPriorSerializer() {
            TypeSerializer<float[]> serializer =
                    TypeExtractor.createTypeInfo(float[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), FloatPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public float[] createTestData() {
            float[] data = new float[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i + 0.2f;
            }
            return data;
        }
    }

    public static final class PrimitiveFloatArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<float[]> {
        @Override
        public TypeSerializer<float[]> createUpgradedSerializer() {
            TypeSerializer<float[]> serializer =
                    TypeExtractor.createTypeInfo(float[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), FloatPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<float[]> testDataMatcher() {
            float[] data = new float[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i + 0.2f;
            }
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<float[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive int array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveIntArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<int[]> {
        @Override
        public TypeSerializer<int[]> createPriorSerializer() {
            TypeSerializer<int[]> serializer =
                    TypeExtractor.createTypeInfo(int[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), IntPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public int[] createTestData() {
            int[] data = new int[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i;
            }
            return data;
        }
    }

    public static final class PrimitiveIntArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<int[]> {
        @Override
        public TypeSerializer<int[]> createUpgradedSerializer() {
            TypeSerializer<int[]> serializer =
                    TypeExtractor.createTypeInfo(int[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), IntPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<int[]> testDataMatcher() {
            int[] data = new int[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i;
            }
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<int[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive long array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveLongArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<long[]> {
        @Override
        public TypeSerializer<long[]> createPriorSerializer() {
            TypeSerializer<long[]> serializer =
                    TypeExtractor.createTypeInfo(long[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), LongPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public long[] createTestData() {
            long[] data = new long[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i;
            }
            return data;
        }
    }

    public static final class PrimitiveLongArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<long[]> {
        @Override
        public TypeSerializer<long[]> createUpgradedSerializer() {
            TypeSerializer<long[]> serializer =
                    TypeExtractor.createTypeInfo(long[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), LongPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<long[]> testDataMatcher() {
            long[] data = new long[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i;
            }
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<long[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive short array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveShortArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<short[]> {
        @Override
        public TypeSerializer<short[]> createPriorSerializer() {
            TypeSerializer<short[]> serializer =
                    TypeExtractor.createTypeInfo(short[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), ShortPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public short[] createTestData() {
            short[] data = new short[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = (short) i;
            }
            return data;
        }
    }

    public static final class PrimitiveShortArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<short[]> {
        @Override
        public TypeSerializer<short[]> createUpgradedSerializer() {
            TypeSerializer<short[]> serializer =
                    TypeExtractor.createTypeInfo(short[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), ShortPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<short[]> testDataMatcher() {
            short[] data = new short[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = (short) i;
            }
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<short[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "primitive string array"
    // ----------------------------------------------------------------------------------------------
    public static final class PrimitiveStringArraySetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<String[]> {
        @Override
        public TypeSerializer<String[]> createPriorSerializer() {
            TypeSerializer<String[]> serializer =
                    TypeExtractor.createTypeInfo(String[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), StringArraySerializer.class);
            return serializer;
        }

        @Override
        public String[] createTestData() {
            String[] data = new String[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = String.valueOf(i);
            }
            return data;
        }
    }

    public static final class PrimitiveStringArrayVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<String[]> {
        @Override
        public TypeSerializer<String[]> createUpgradedSerializer() {
            TypeSerializer<String[]> serializer =
                    TypeExtractor.createTypeInfo(String[].class)
                            .createSerializer(new ExecutionConfig());
            assertSame(serializer.getClass(), StringArraySerializer.class);
            return serializer;
        }

        @Override
        public Matcher<String[]> testDataMatcher() {
            String[] data = new String[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = String.valueOf(i);
            }
            return is(data);
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<String[]>> schemaCompatibilityMatcher(
                MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }
}
