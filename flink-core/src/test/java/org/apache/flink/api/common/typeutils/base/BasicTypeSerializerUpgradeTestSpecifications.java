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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConditions;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import org.assertj.core.api.Condition;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

/** Test specifications for {@link BasicTypeSerializerUpgradeTest}. */
public class BasicTypeSerializerUpgradeTestSpecifications {
    // ----------------------------------------------------------------------------------------------
    // Specification for "big-dec-serializer"
    // ----------------------------------------------------------------------------------------------
    /** BigDecSerializerSetup. */
    public static final class BigDecSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<BigDecimal> {
        @Override
        public TypeSerializer<BigDecimal> createPriorSerializer() {
            return BigDecSerializer.INSTANCE;
        }

        @Override
        public BigDecimal createTestData() {
            return new BigDecimal("123456789012345678901234567890123456.789");
        }
    }

    /** BigDecSerializerVerifier. */
    public static final class BigDecSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<BigDecimal> {
        @Override
        public TypeSerializer<BigDecimal> createUpgradedSerializer() {
            return BigDecSerializer.INSTANCE;
        }

        @Override
        public Condition<BigDecimal> testDataCondition() {
            return new Condition<>(
                    value ->
                            value.equals(
                                    new BigDecimal("123456789012345678901234567890123456.789")),
                    "value is 123456789012345678901234567890123456.789");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<BigDecimal>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "big-int-serializer"
    // ----------------------------------------------------------------------------------------------
    /** BigIntSerializerSetup. */
    public static final class BigIntSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<BigInteger> {
        @Override
        public TypeSerializer<BigInteger> createPriorSerializer() {
            return BigIntSerializer.INSTANCE;
        }

        @Override
        public BigInteger createTestData() {
            return new BigInteger("123456789012345678901234567890123456");
        }
    }

    /** BigIntSerializerVerifier. */
    public static final class BigIntSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<BigInteger> {
        @Override
        public TypeSerializer<BigInteger> createUpgradedSerializer() {
            return BigIntSerializer.INSTANCE;
        }

        @Override
        public Condition<BigInteger> testDataCondition() {
            return new Condition<>(
                    value -> value.equals(new BigInteger("123456789012345678901234567890123456")),
                    "value is 123456789012345678901234567890123456");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<BigInteger>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "BooleanSerializer"
    // ----------------------------------------------------------------------------------------------
    /** BooleanSerializerSetup. */
    public static final class BooleanSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Boolean> {
        @Override
        public TypeSerializer<Boolean> createPriorSerializer() {
            return BooleanSerializer.INSTANCE;
        }

        @Override
        public Boolean createTestData() {
            return Boolean.TRUE;
        }
    }

    /** BooleanSerializerVerifier. */
    public static final class BooleanSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Boolean> {
        @Override
        public TypeSerializer<Boolean> createUpgradedSerializer() {
            return BooleanSerializer.INSTANCE;
        }

        @Override
        public Condition<Boolean> testDataCondition() {
            return new Condition<>(Boolean.TRUE::equals, "value is true");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Boolean>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "boolean-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** BooleanValueSerializerSetup. */
    public static final class BooleanValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<BooleanValue> {
        @Override
        public TypeSerializer<BooleanValue> createPriorSerializer() {
            return BooleanValueSerializer.INSTANCE;
        }

        @Override
        public BooleanValue createTestData() {
            return BooleanValue.TRUE;
        }
    }

    /** BooleanValueSerializerVerifier. */
    public static final class BooleanValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<BooleanValue> {
        @Override
        public TypeSerializer<BooleanValue> createUpgradedSerializer() {
            return BooleanValueSerializer.INSTANCE;
        }

        @Override
        public Condition<BooleanValue> testDataCondition() {
            return new Condition<>(BooleanValue.TRUE::equals, "value is true");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<BooleanValue>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "byte-serializer"
    // ----------------------------------------------------------------------------------------------
    /** ByteSerializerSetup. */
    public static final class ByteSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Byte> {
        @Override
        public TypeSerializer<Byte> createPriorSerializer() {
            return ByteSerializer.INSTANCE;
        }

        @Override
        public Byte createTestData() {
            return Byte.valueOf("42");
        }
    }

    /** ByteSerializerVerifier. */
    public static final class ByteSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Byte> {
        @Override
        public TypeSerializer<Byte> createUpgradedSerializer() {
            return ByteSerializer.INSTANCE;
        }

        @Override
        public Condition<Byte> testDataCondition() {
            return new Condition<>(value -> value.equals(Byte.valueOf("42")), "value is 42");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Byte>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "byte-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** ByteValueSerializerSetup. */
    public static final class ByteValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ByteValue> {
        @Override
        public TypeSerializer<ByteValue> createPriorSerializer() {
            return ByteValueSerializer.INSTANCE;
        }

        @Override
        public ByteValue createTestData() {
            return new ByteValue((byte) 42);
        }
    }

    /** ByteValueSerializerVerifier. */
    public static final class ByteValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ByteValue> {
        @Override
        public TypeSerializer<ByteValue> createUpgradedSerializer() {
            return ByteValueSerializer.INSTANCE;
        }

        @Override
        public Condition<ByteValue> testDataCondition() {
            return new Condition<>(new ByteValue((byte) 42)::equals, "value is 42");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<ByteValue>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "char-serializer"
    // ----------------------------------------------------------------------------------------------
    /** CharSerializerSetup. */
    public static final class CharSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Character> {
        @Override
        public TypeSerializer<Character> createPriorSerializer() {
            return CharSerializer.INSTANCE;
        }

        @Override
        public Character createTestData() {
            return Character.MAX_VALUE;
        }
    }

    /** CharSerializerVerifier. */
    public static final class CharSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Character> {
        @Override
        public TypeSerializer<Character> createUpgradedSerializer() {
            return CharSerializer.INSTANCE;
        }

        @Override
        public Condition<Character> testDataCondition() {
            return new Condition<>(
                    value -> value.equals(Character.MAX_VALUE), "value is Character.MAX_VALUE");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Character>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "char-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** CharValueSerializerSetup. */
    public static final class CharValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<CharValue> {
        @Override
        public TypeSerializer<CharValue> createPriorSerializer() {
            return CharValueSerializer.INSTANCE;
        }

        @Override
        public CharValue createTestData() {
            return new CharValue((char) 42);
        }
    }

    /** CharValueSerializerVerifier. */
    public static final class CharValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<CharValue> {
        @Override
        public TypeSerializer<CharValue> createUpgradedSerializer() {
            return CharValueSerializer.INSTANCE;
        }

        @Override
        public Condition<CharValue> testDataCondition() {
            return new Condition<>(new CharValue((char) 42)::equals, "value is 42");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<CharValue>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "date-serializer"
    // ----------------------------------------------------------------------------------------------
    /** DateSerializerSetup. */
    public static final class DateSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Date> {
        @Override
        public TypeSerializer<Date> createPriorSerializer() {
            return DateSerializer.INSTANCE;
        }

        @Override
        public Date createTestData() {
            return new Date(1580382960L);
        }
    }

    /** DateSerializerVerifier. */
    public static final class DateSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Date> {
        @Override
        public TypeSerializer<Date> createUpgradedSerializer() {
            return DateSerializer.INSTANCE;
        }

        @Override
        public Condition<Date> testDataCondition() {
            return new Condition<>(new Date(1580382960L)::equals, "value is 1580382960L");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Date>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "double-serializer"
    // ----------------------------------------------------------------------------------------------
    /** DoubleSerializerSetup. */
    public static final class DoubleSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Double> {
        @Override
        public TypeSerializer<Double> createPriorSerializer() {
            return DoubleSerializer.INSTANCE;
        }

        @Override
        public Double createTestData() {
            return new Double("12345.6789");
        }
    }

    /** DoubleSerializerVerifier. */
    public static final class DoubleSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Double> {
        @Override
        public TypeSerializer<Double> createUpgradedSerializer() {
            return DoubleSerializer.INSTANCE;
        }

        @Override
        public Condition<Double> testDataCondition() {
            return new Condition<>(new Double("12345.6789")::equals, "value is 12345.6789");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Double>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "double-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** DoubleValueSerializerSetup. */
    public static final class DoubleValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<DoubleValue> {
        @Override
        public TypeSerializer<DoubleValue> createPriorSerializer() {
            return DoubleValueSerializer.INSTANCE;
        }

        @Override
        public DoubleValue createTestData() {
            return new DoubleValue(12345.6789);
        }
    }

    /** DoubleValueSerializerVerifier. */
    public static final class DoubleValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<DoubleValue> {
        @Override
        public TypeSerializer<DoubleValue> createUpgradedSerializer() {
            return DoubleValueSerializer.INSTANCE;
        }

        @Override
        public Condition<DoubleValue> testDataCondition() {
            return new Condition<>(new DoubleValue(12345.6789)::equals, "value is 12345.6789");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<DoubleValue>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "float-serializer"
    // ----------------------------------------------------------------------------------------------
    /** FloatSerializerSetup. */
    public static final class FloatSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Float> {
        @Override
        public TypeSerializer<Float> createPriorSerializer() {
            return FloatSerializer.INSTANCE;
        }

        @Override
        public Float createTestData() {
            return new Float("123.456");
        }
    }

    /** FloatSerializerVerifier. */
    public static final class FloatSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Float> {
        @Override
        public TypeSerializer<Float> createUpgradedSerializer() {
            return FloatSerializer.INSTANCE;
        }

        @Override
        public Condition<Float> testDataCondition() {
            return new Condition<>(new Float("123.456")::equals, "value is 123.456");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Float>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "float-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** FloatValueSerializerSetup. */
    public static final class FloatValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<FloatValue> {
        @Override
        public TypeSerializer<FloatValue> createPriorSerializer() {
            return FloatValueSerializer.INSTANCE;
        }

        @Override
        public FloatValue createTestData() {
            return new FloatValue(123.456f);
        }
    }

    /** FloatValueSerializerVerifier. */
    public static final class FloatValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<FloatValue> {
        @Override
        public TypeSerializer<FloatValue> createUpgradedSerializer() {
            return FloatValueSerializer.INSTANCE;
        }

        @Override
        public Condition<FloatValue> testDataCondition() {
            return new Condition<>(new FloatValue(123.456f)::equals, "value is 123.456f");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<FloatValue>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "int-serializer"
    // ----------------------------------------------------------------------------------------------
    /** IntSerializerSetup. */
    public static final class IntSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Integer> {
        @Override
        public TypeSerializer<Integer> createPriorSerializer() {
            return IntSerializer.INSTANCE;
        }

        @Override
        public Integer createTestData() {
            return 123456;
        }
    }

    /** IntSerializerVerifier. */
    public static final class IntSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Integer> {
        @Override
        public TypeSerializer<Integer> createUpgradedSerializer() {
            return IntSerializer.INSTANCE;
        }

        @Override
        public Condition<Integer> testDataCondition() {
            return new Condition<>(value -> value.equals(123456), "value is 123456");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Integer>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "int-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** IntValueSerializerSetup. */
    public static final class IntValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<IntValue> {
        @Override
        public TypeSerializer<IntValue> createPriorSerializer() {
            return IntValueSerializer.INSTANCE;
        }

        @Override
        public IntValue createTestData() {
            return new IntValue(123456);
        }
    }

    /** IntValueSerializerVerifier. */
    public static final class IntValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<IntValue> {
        @Override
        public TypeSerializer<IntValue> createUpgradedSerializer() {
            return IntValueSerializer.INSTANCE;
        }

        @Override
        public Condition<IntValue> testDataCondition() {
            return new Condition<>(new IntValue(123456)::equals, "value is 123456");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<IntValue>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "long-serializer"
    // ----------------------------------------------------------------------------------------------
    /** LongSerializerSetup. */
    public static final class LongSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Long> {
        @Override
        public TypeSerializer<Long> createPriorSerializer() {
            return LongSerializer.INSTANCE;
        }

        @Override
        public Long createTestData() {
            return 1234567890L;
        }
    }

    /** LongSerializerVerifier. */
    public static final class LongSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Long> {
        @Override
        public TypeSerializer<Long> createUpgradedSerializer() {
            return LongSerializer.INSTANCE;
        }

        @Override
        public Condition<Long> testDataCondition() {
            return new Condition<>(value -> value.equals(1234567890L), "value is 1234567890L");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Long>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "long-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** LongValueSerializerSetup. */
    public static final class LongValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<LongValue> {
        @Override
        public TypeSerializer<LongValue> createPriorSerializer() {
            return LongValueSerializer.INSTANCE;
        }

        @Override
        public LongValue createTestData() {
            return new LongValue(1234567890);
        }
    }

    /** LongValueSerializerVerifier. */
    public static final class LongValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<LongValue> {
        @Override
        public TypeSerializer<LongValue> createUpgradedSerializer() {
            return LongValueSerializer.INSTANCE;
        }

        @Override
        public Condition<LongValue> testDataCondition() {
            return new Condition<>(new LongValue(1234567890)::equals, "value is 1234567890");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<LongValue>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "null-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** NullValueSerializerSetup. */
    public static final class NullValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<NullValue> {
        @Override
        public TypeSerializer<NullValue> createPriorSerializer() {
            return NullValueSerializer.INSTANCE;
        }

        @Override
        public NullValue createTestData() {
            return NullValue.getInstance();
        }
    }

    /** NullValueSerializerVerifier. */
    public static final class NullValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<NullValue> {
        @Override
        public TypeSerializer<NullValue> createUpgradedSerializer() {
            return NullValueSerializer.INSTANCE;
        }

        @Override
        public Condition<NullValue> testDataCondition() {
            return new Condition<>(
                    NullValue.getInstance()::equals, "value is NullValue.getInstance()");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<NullValue>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "short-serializer"
    // ----------------------------------------------------------------------------------------------
    /** ShortSerializerSetup. */
    public static final class ShortSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Short> {
        @Override
        public TypeSerializer<Short> createPriorSerializer() {
            return ShortSerializer.INSTANCE;
        }

        @Override
        public Short createTestData() {
            return 123;
        }
    }

    /** ShortSerializerVerifier. */
    public static final class ShortSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Short> {
        @Override
        public TypeSerializer<Short> createUpgradedSerializer() {
            return ShortSerializer.INSTANCE;
        }

        @Override
        public Condition<Short> testDataCondition() {
            return new Condition<>(value -> value == 123, "value is 123");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Short>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "short-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** ShortValueSerializerSetup. */
    public static final class ShortValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ShortValue> {
        @Override
        public TypeSerializer<ShortValue> createPriorSerializer() {
            return ShortValueSerializer.INSTANCE;
        }

        @Override
        public ShortValue createTestData() {
            return new ShortValue((short) 123);
        }
    }

    /** ShortValueSerializerVerifier. */
    public static final class ShortValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ShortValue> {
        @Override
        public TypeSerializer<ShortValue> createUpgradedSerializer() {
            return ShortValueSerializer.INSTANCE;
        }

        @Override
        public Condition<ShortValue> testDataCondition() {
            return new Condition<>(new ShortValue((short) 123)::equals, "value is 123");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<ShortValue>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "sql-date-serializer"
    // ----------------------------------------------------------------------------------------------
    /** SqlDateSerializerSetup. */
    public static final class SqlDateSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<java.sql.Date> {
        @Override
        public TypeSerializer<java.sql.Date> createPriorSerializer() {
            return SqlDateSerializer.INSTANCE;
        }

        @Override
        public java.sql.Date createTestData() {
            return new java.sql.Date(1580382960L);
        }
    }

    /** SqlDateSerializerVerifier. */
    public static final class SqlDateSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<java.sql.Date> {
        @Override
        public TypeSerializer<java.sql.Date> createUpgradedSerializer() {
            return SqlDateSerializer.INSTANCE;
        }

        @Override
        public Condition<java.sql.Date> testDataCondition() {
            return new Condition<>(
                    value -> value.equals(new java.sql.Date(1580382960L)), "value is 1580382960L");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<java.sql.Date>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "sql-time-serializer"
    // ----------------------------------------------------------------------------------------------
    /** SqlTimeSerializerSetup. */
    public static final class SqlTimeSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Time> {
        @Override
        public TypeSerializer<Time> createPriorSerializer() {
            return SqlTimeSerializer.INSTANCE;
        }

        @Override
        public Time createTestData() {
            return new Time(1580382960L);
        }
    }

    /** SqlTimeSerializerVerifier. */
    public static final class SqlTimeSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Time> {
        @Override
        public TypeSerializer<Time> createUpgradedSerializer() {
            return SqlTimeSerializer.INSTANCE;
        }

        @Override
        public Condition<Time> testDataCondition() {
            return new Condition<>(
                    value -> value.equals(new Time(1580382960L)), "value is 1580382960L");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Time>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "sql-timestamp-serializer"
    // ----------------------------------------------------------------------------------------------
    /** SqlTimestampSerializerSetup. */
    public static final class SqlTimestampSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Timestamp> {
        @Override
        public TypeSerializer<Timestamp> createPriorSerializer() {
            return SqlTimestampSerializer.INSTANCE;
        }

        @Override
        public Timestamp createTestData() {
            return new Timestamp(1580382960L);
        }
    }

    /** SqlTimestampSerializerVerifier. */
    public static final class SqlTimestampSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Timestamp> {
        @Override
        public TypeSerializer<Timestamp> createUpgradedSerializer() {
            return SqlTimestampSerializer.INSTANCE;
        }

        @Override
        public Condition<Timestamp> testDataCondition() {
            return new Condition<>(
                    value -> value.equals(new Timestamp(1580382960L)), "value is 1580382960L");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<Timestamp>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "string-serializer"
    // ----------------------------------------------------------------------------------------------
    /** StringSerializerSetup. */
    public static final class StringSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<String> {
        @Override
        public TypeSerializer<String> createPriorSerializer() {
            return StringSerializer.INSTANCE;
        }

        @Override
        public String createTestData() {
            return "123456789012345678901234567890123456";
        }
    }

    /** StringSerializerVerifier. */
    public static final class StringSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<String> {
        @Override
        public TypeSerializer<String> createUpgradedSerializer() {
            return StringSerializer.INSTANCE;
        }

        @Override
        public Condition<String> testDataCondition() {
            return new Condition<>(
                    value -> value.equals("123456789012345678901234567890123456"),
                    "value is 123456789012345678901234567890123456");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<String>> schemaCompatibilityCondition(
                FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Specification for "string-value-serializer"
    // ----------------------------------------------------------------------------------------------
    /** StringValueSerializerSetup. */
    public static final class StringValueSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StringValue> {
        @Override
        public TypeSerializer<StringValue> createPriorSerializer() {
            return StringValueSerializer.INSTANCE;
        }

        @Override
        public StringValue createTestData() {
            return new StringValue("123456789012345678901234567890123456");
        }
    }

    /** StringValueSerializerVerifier. */
    public static final class StringValueSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StringValue> {
        @Override
        public TypeSerializer<StringValue> createUpgradedSerializer() {
            return StringValueSerializer.INSTANCE;
        }

        @Override
        public Condition<StringValue> testDataCondition() {
            return new Condition<>(
                    new StringValue("123456789012345678901234567890123456")::equals,
                    "value is 123456789012345678901234567890123456");
        }

        @Override
        public Condition<TypeSerializerSchemaCompatibility<StringValue>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
