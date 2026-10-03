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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.STRUCTURED;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.UUID;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.DataTypes.VARIANT;
import static org.apache.flink.table.types.logical.VarCharType.STRING_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

class CastRuleProviderTest {

    private static final LogicalType DISTINCT_INT =
            DistinctType.newBuilder(ObjectIdentifier.of("a", "b", "c"), INT().getLogicalType())
                    .build();
    private static final LogicalType DISTINCT_BIG_INT =
            DistinctType.newBuilder(ObjectIdentifier.of("a", "b", "c"), BIGINT().getLogicalType())
                    .build();
    private static final LogicalType INT = INT().getLogicalType();
    private static final LogicalType TINYINT = TINYINT().getLogicalType();
    private static final LogicalType VARIANT = VARIANT().getLogicalType();
    private static final LogicalType ROW =
            ROW(FIELD("a", INT()), FIELD("b", TINYINT().notNull())).getLogicalType();
    private static final LogicalType STRUCTURED =
            STRUCTURED("Obj", FIELD("a", INT()), FIELD("b", TINYINT().notNull())).getLogicalType();

    @Test
    void testResolveDistinctTypeToIdentityCastRule() {
        assertThat(CastRuleProvider.resolve(DISTINCT_INT, INT)).isSameAs(IdentityCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(INT, DISTINCT_INT)).isSameAs(IdentityCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(DISTINCT_INT, DISTINCT_INT))
                .isSameAs(IdentityCastRule.INSTANCE);
    }

    @Test
    void testResolveCompatiblesTypesToIdentityCastRule() {
        assertThat(CastRuleProvider.resolve(INT, INT)).isSameAs(IdentityCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(ROW, ROW)).isSameAs(IdentityCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(ROW, STRUCTURED)).isSameAs(IdentityCastRule.INSTANCE);
    }

    @Test
    void testResolveIntToBigIntWithDistinct() {
        assertThat(CastRuleProvider.resolve(INT, DISTINCT_BIG_INT))
                .isSameAs(NumericPrimitiveCastRule.INSTANCE);
    }

    @Test
    void testResolveArrayIntToBigIntWithDistinct() {
        assertThat(CastRuleProvider.resolve(new ArrayType(INT), new ArrayType(DISTINCT_BIG_INT)))
                .isSameAs(ArrayToArrayCastRule.INSTANCE);
    }

    @Test
    void testResolvePredefinedToString() {
        assertThat(CastRuleProvider.resolve(INT, new VarCharType(10)))
                .isSameAs(CharVarCharTrimPadCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(INT, new CharType(10)))
                .isSameAs(CharVarCharTrimPadCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(INT, STRING_TYPE))
                .isSameAs(NumericToStringCastRule.INSTANCE);
    }

    @Test
    void testResolveConstructedToString() {
        assertThat(CastRuleProvider.resolve(new ArrayType(INT), new VarCharType(10)))
                .isSameAs(ArrayToStringCastRule.INSTANCE);
    }

    @Test
    void testCanFail() {
        assertThat(CastRuleProvider.canFail(TINYINT, INT)).isFalse();
        assertThat(CastRuleProvider.canFail(STRING_TYPE, TIME().getLogicalType())).isTrue();
        assertThat(CastRuleProvider.canFail(STRING_TYPE, STRING_TYPE)).isFalse();

        LogicalType inputType = ROW(TINYINT(), STRING()).getLogicalType();
        assertThat(CastRuleProvider.canFail(inputType, ROW(INT(), TIME()).getLogicalType()))
                .isTrue();
        assertThat(CastRuleProvider.canFail(inputType, ROW(INT(), STRING()).getLogicalType()))
                .isFalse();
    }

    @Test
    void testResolveVariantToPrimitive() {
        assertThat(CastRuleProvider.resolve(VARIANT, INT))
                .isSameAs(VariantToPrimitiveCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(VARIANT, BOOLEAN().getLogicalType()))
                .isSameAs(VariantToPrimitiveCastRule.INSTANCE);
        assertThat(CastRuleProvider.exists(VARIANT, DECIMAL(10, 2).getLogicalType())).isTrue();
        assertThat(CastRuleProvider.exists(VARIANT, DATE().getLogicalType())).isTrue();
        assertThat(CastRuleProvider.exists(VARIANT, TIMESTAMP().getLogicalType())).isTrue();
        assertThat(CastRuleProvider.exists(VARIANT, TIMESTAMP_LTZ().getLogicalType())).isTrue();
        assertThat(CastRuleProvider.exists(VARIANT, TIME().getLogicalType())).isTrue();
        assertThat(CastRuleProvider.exists(VARIANT, BYTES().getLogicalType())).isTrue();
        assertThat(CastRuleProvider.exists(VARIANT, UUID().getLogicalType())).isTrue();
        assertThat(CastRuleProvider.canFail(VARIANT, INT)).isTrue();

        // INTERVAL has no VARIANT counterpart, so it is not a castable target
        assertThat(CastRuleProvider.exists(VARIANT, INTERVAL(DataTypes.DAY()).getLogicalType()))
                .isFalse();
        // character strings keep going through the display-oriented rule
        assertThat(CastRuleProvider.resolve(VARIANT, STRING_TYPE))
                .isSameAs(VariantToStringCastRule.INSTANCE);
    }

    @Test
    void testResolvePrimitiveToVariant() {
        assertThat(CastRuleProvider.resolve(INT, VARIANT))
                .isSameAs(PrimitiveToVariantCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(STRING_TYPE, VARIANT))
                .isSameAs(PrimitiveToVariantCastRule.INSTANCE);
        assertThat(CastRuleProvider.exists(DECIMAL(10, 2).getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.exists(TIMESTAMP(9).getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.exists(TIMESTAMP_LTZ().getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.exists(TIME().getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.exists(BYTES().getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.exists(UUID().getLogicalType(), VARIANT)).isTrue();

        // only a nanosecond timestamp can fail, since that kind covers a limited range of years
        assertThat(CastRuleProvider.canFail(INT, VARIANT)).isFalse();
        assertThat(CastRuleProvider.canFail(DOUBLE().getLogicalType(), VARIANT)).isFalse();
        assertThat(CastRuleProvider.canFail(FLOAT().getLogicalType(), VARIANT)).isFalse();
        assertThat(CastRuleProvider.canFail(TIMESTAMP(6).getLogicalType(), VARIANT)).isFalse();
        assertThat(CastRuleProvider.canFail(TIMESTAMP(9).getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.canFail(TIMESTAMP_LTZ(3).getLogicalType(), VARIANT)).isFalse();
        assertThat(CastRuleProvider.canFail(TIMESTAMP_LTZ(7).getLogicalType(), VARIANT)).isTrue();

        // a string or binary type fails only when its declared length allows a value over 16 MiB,
        // and a character takes up to 4 bytes in UTF-8
        assertThat(CastRuleProvider.canFail(STRING_TYPE, VARIANT)).isTrue();
        assertThat(CastRuleProvider.canFail(BYTES().getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.canFail(VARCHAR(100).getLogicalType(), VARIANT)).isFalse();
        assertThat(CastRuleProvider.canFail(CHAR(100).getLogicalType(), VARIANT)).isFalse();
        assertThat(CastRuleProvider.canFail(VARCHAR(4_194_302).getLogicalType(), VARIANT))
                .isFalse();
        assertThat(CastRuleProvider.canFail(VARCHAR(4_194_303).getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.canFail(VARBINARY(16_777_211).getLogicalType(), VARIANT))
                .isFalse();
        assertThat(CastRuleProvider.canFail(VARBINARY(16_777_212).getLogicalType(), VARIANT))
                .isTrue();

        // INTERVAL has no VARIANT kind, so it is not a castable source
        assertThat(CastRuleProvider.exists(INTERVAL(DataTypes.DAY()).getLogicalType(), VARIANT))
                .isFalse();
        // VARIANT to VARIANT stays the identity
        assertThat(CastRuleProvider.resolve(VARIANT, VARIANT)).isSameAs(IdentityCastRule.INSTANCE);

        // a constructed type casts element by element, through the rules for its children
        assertThat(
                        CastRuleProvider.resolve(
                                ARRAY(INT()).getLogicalType(), ARRAY(VARIANT()).getLogicalType()))
                .isSameAs(ArrayToArrayCastRule.INSTANCE);
        assertThat(
                        CastRuleProvider.resolve(
                                ROW(FIELD("a", INT())).getLogicalType(),
                                ROW(FIELD("a", VARIANT())).getLogicalType()))
                .isSameAs(RowToRowCastRule.INSTANCE);
        assertThat(
                        CastRuleProvider.resolve(
                                MAP(STRING(), INT()).getLogicalType(),
                                MAP(STRING(), VARIANT()).getLogicalType()))
                .isSameAs(MapToMapAndMultisetToMultisetCastRule.INSTANCE);
        assertThat(
                        CastRuleProvider.canFail(
                                ARRAY(INT()).getLogicalType(), ARRAY(VARIANT()).getLogicalType()))
                .isFalse();
        assertThat(
                        CastRuleProvider.canFail(
                                ARRAY(TIMESTAMP(9)).getLogicalType(),
                                ARRAY(VARIANT()).getLogicalType()))
                .isTrue();
    }

    @Test
    void testResolveConstructedToVariant() {
        assertThat(CastRuleProvider.resolve(ARRAY(INT()).getLogicalType(), VARIANT))
                .isSameAs(ConstructedToVariantCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(MAP(STRING(), INT()).getLogicalType(), VARIANT))
                .isSameAs(ConstructedToVariantCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(ROW, VARIANT))
                .isSameAs(ConstructedToVariantCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(STRUCTURED, VARIANT))
                .isSameAs(ConstructedToVariantCastRule.INSTANCE);

        // every leaf needs a VARIANT kind, and a map key is never converted to a string
        assertThat(CastRuleProvider.exists(ARRAY(INTERVAL(MONTH())).getLogicalType(), VARIANT))
                .isFalse();
        assertThat(CastRuleProvider.exists(MAP(INT(), STRING()).getLogicalType(), VARIANT))
                .isFalse();
        assertThat(CastRuleProvider.exists(MULTISET(STRING()).getLogicalType(), VARIANT)).isFalse();

        // any ARRAY, MAP or nested VARIANT can exceed 16 MiB
        assertThat(CastRuleProvider.canFail(ARRAY(INT()).getLogicalType(), VARIANT)).isTrue();
        assertThat(CastRuleProvider.canFail(MAP(STRING(), INT()).getLogicalType(), VARIANT))
                .isTrue();
        assertThat(CastRuleProvider.canFail(ROW(FIELD("v", VARIANT())).getLogicalType(), VARIANT))
                .isTrue();

        // a ROW fails when a leaf can, or when the declared sizes of its fields add up past 16 MiB
        assertThat(CastRuleProvider.canFail(ROW, VARIANT)).isFalse();
        assertThat(CastRuleProvider.canFail(STRUCTURED, VARIANT)).isFalse();
        assertThat(
                        CastRuleProvider.canFail(
                                ROW(FIELD("a", TIMESTAMP(9))).getLogicalType(), VARIANT))
                .isTrue();
        assertThat(CastRuleProvider.canFail(ROW(FIELD("a", STRING())).getLogicalType(), VARIANT))
                .isTrue();
        assertThat(
                        CastRuleProvider.canFail(
                                ROW(FIELD("a", VARCHAR(4_000_000))).getLogicalType(), VARIANT))
                .isFalse();
        assertThat(
                        CastRuleProvider.canFail(
                                ROW(FIELD("a", VARCHAR(4_000_000)), FIELD("b", VARCHAR(4_000_000)))
                                        .getLogicalType(),
                                VARIANT))
                .isTrue();
    }

    @Test
    void testResolveVariantToArray() {
        assertThat(CastRuleProvider.resolve(VARIANT, ARRAY(INT()).getLogicalType()))
                .isSameAs(VariantToArrayCastRule.INSTANCE);

        // the element recurses through the VARIANT rules, including the identity leaf and nesting
        assertThat(CastRuleProvider.exists(VARIANT, ARRAY(VARIANT()).getLogicalType())).isTrue();
        assertThat(CastRuleProvider.exists(VARIANT, ARRAY(ARRAY(INT())).getLogicalType())).isTrue();
        assertThat(CastRuleProvider.canFail(VARIANT, ARRAY(INT()).getLogicalType())).isTrue();

        // an element with no variant counterpart makes the whole cast unresolvable
        assertThat(CastRuleProvider.exists(VARIANT, ARRAY(INTERVAL(MONTH())).getLogicalType()))
                .isFalse();
        // MULTISET has no variant counterpart
        assertThat(CastRuleProvider.exists(VARIANT, MULTISET(STRING()).getLogicalType())).isFalse();
    }

    @Test
    void testResolveVariantToRow() {
        assertThat(CastRuleProvider.resolve(VARIANT, ROW(FIELD("f0", INT())).getLogicalType()))
                .isSameAs(VariantToRowCastRule.INSTANCE);
        // a structured target shares the ROW rule
        assertThat(CastRuleProvider.resolve(VARIANT, STRUCTURED))
                .isSameAs(VariantToRowCastRule.INSTANCE);

        // a field with no variant counterpart makes the whole cast unresolvable
        assertThat(
                        CastRuleProvider.exists(
                                VARIANT, ROW(FIELD("f0", MULTISET(STRING()))).getLogicalType()))
                .isFalse();
    }

    @Test
    void testResolveVariantToMap() {
        assertThat(CastRuleProvider.resolve(VARIANT, MAP(STRING(), INT()).getLogicalType()))
                .isSameAs(VariantToMapCastRule.INSTANCE);
        // the value recurses through the VARIANT rules, including the identity leaf
        assertThat(CastRuleProvider.exists(VARIANT, MAP(STRING(), VARIANT()).getLogicalType()))
                .isTrue();

        // a non-string map key is rejected
        assertThat(CastRuleProvider.exists(VARIANT, MAP(INT(), STRING()).getLogicalType()))
                .isFalse();
    }
}
