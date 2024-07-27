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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.assertj.core.api.Condition;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(BooleanPrimitiveArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(BooleanPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<boolean[]> testDataCondition() {
            boolean[] expected = new boolean[2];
            expected[0] = true;
            expected[1] = false;

            return new Condition<>(
                    value -> Arrays.equals(expected, value),
                    "value is " + Arrays.toString(expected));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<boolean[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(BytePrimitiveArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(BytePrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<byte[]> testDataCondition() {
            byte[] expected = new byte[10];
            for (int i = 0; i < expected.length; ++i) {
                expected[i] = (byte) i;
            }
            return new Condition<>(
                    value -> Arrays.equals(expected, value),
                    "value is " + Arrays.toString(expected));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<byte[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(CharPrimitiveArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(CharPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<char[]> testDataCondition() {
            char[] data = new char[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = (char) i;
            }
            return new Condition<>(
                    value -> Arrays.equals(data, value), "data is [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<char[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(DoublePrimitiveArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(DoublePrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<double[]> testDataCondition() {
            double[] data = new double[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i + 0.1f;
            }
            return new Condition<>(
                    value -> Arrays.equals(data, value), "data is " + Arrays.toString(data));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<double[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(FloatPrimitiveArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(FloatPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<float[]> testDataCondition() {
            float[] data = new float[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i + 0.2f;
            }
            return new Condition<>(
                    value -> Arrays.equals(data, value), "data is " + Arrays.toString(data));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<float[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(IntPrimitiveArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(IntPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<int[]> testDataCondition() {
            int[] data = new int[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i;
            }
            return new Condition<>(
                    value -> Arrays.equals(data, value), "data is " + Arrays.toString(data));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<int[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(LongPrimitiveArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(LongPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<long[]> testDataCondition() {
            long[] data = new long[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = i;
            }
            return new Condition<>(
                    value -> Arrays.equals(data, value), "data is " + Arrays.toString(data));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<long[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(ShortPrimitiveArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(ShortPrimitiveArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<short[]> testDataCondition() {
            short[] data = new short[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = (short) i;
            }
            return new Condition<>(
                    value -> Arrays.equals(data, value), "data is " + Arrays.toString(data));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<short[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(StringArraySerializer.class);
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
                            .createSerializer(new SerializerConfigImpl());
            assertThat(serializer.getClass()).isSameAs(StringArraySerializer.class);
            return serializer;
        }

        @Override
        public Condition<String[]> testDataCondition() {
            String[] data = new String[10];
            for (int i = 0; i < data.length; ++i) {
                data[i] = String.valueOf(i);
            }
            return new Condition<>(
                    value -> Arrays.equals(data, value), "data is " + Arrays.toString(data));
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<String[]>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
