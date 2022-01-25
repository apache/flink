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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for serialization/deserialization of {@link RexNode}. */
public class RexNodeSerdeTest {

    private static final FlinkTypeFactory FACTORY = FlinkTypeFactory.INSTANCE();

    @ParameterizedTest
    @MethodSource("testRexNodeSerde")
    public void testRexNodeSerde(RexNode rexNode) throws IOException {
        final SerdeContext serdeContext = configuredSerdeContext();
        final String json = toJson(serdeContext, rexNode);
        final RexNode actual = toObject(serdeContext, json, RexNode.class);

        assertThat(actual).isEqualTo(rexNode);
    }

    // --------------------------------------------------------------------------------------------
    // Test data
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("UnstableApiUsage")
    private static Stream<RexNode> testRexNodeSerde() {
        final RexBuilder rexBuilder = new RexBuilder(FACTORY);
        final RelDataType inputType =
                FACTORY.createStructType(
                        StructKind.PEEK_FIELDS_NO_EXPAND,
                        Arrays.asList(
                                FACTORY.createSqlType(SqlTypeName.INTEGER),
                                FACTORY.createSqlType(SqlTypeName.BIGINT),
                                FACTORY.createStructType(
                                        StructKind.PEEK_FIELDS_NO_EXPAND,
                                        Arrays.asList(
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR),
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR)),
                                        Arrays.asList("n1", "n2"))),
                        Arrays.asList("f1", "f2", "f3"));

        return Stream.of(
                rexBuilder.makeNullLiteral(FACTORY.createSqlType(SqlTypeName.VARCHAR)),
                rexBuilder.makeLiteral(true),
                rexBuilder.makeExactLiteral(
                        new BigDecimal(Byte.MAX_VALUE), FACTORY.createSqlType(SqlTypeName.TINYINT)),
                rexBuilder.makeExactLiteral(
                        new BigDecimal(Short.MAX_VALUE),
                        FACTORY.createSqlType(SqlTypeName.SMALLINT)),
                rexBuilder.makeExactLiteral(
                        new BigDecimal(Integer.MAX_VALUE),
                        FACTORY.createSqlType(SqlTypeName.INTEGER)),
                rexBuilder.makeExactLiteral(
                        new BigDecimal(Long.MAX_VALUE), FACTORY.createSqlType(SqlTypeName.BIGINT)),
                rexBuilder.makeExactLiteral(
                        BigDecimal.valueOf(Double.MAX_VALUE),
                        FACTORY.createSqlType(SqlTypeName.DOUBLE)),
                rexBuilder.makeApproxLiteral(
                        BigDecimal.valueOf(Float.MAX_VALUE),
                        FACTORY.createSqlType(SqlTypeName.FLOAT)),
                rexBuilder.makeExactLiteral(new BigDecimal("23.1234567890123456789012345678")),
                rexBuilder.makeIntervalLiteral(
                        BigDecimal.valueOf(100),
                        new SqlIntervalQualifier(
                                TimeUnit.YEAR,
                                4,
                                TimeUnit.YEAR,
                                RelDataType.PRECISION_NOT_SPECIFIED,
                                SqlParserPos.ZERO)),
                rexBuilder.makeIntervalLiteral(
                        BigDecimal.valueOf(3),
                        new SqlIntervalQualifier(
                                TimeUnit.YEAR,
                                2,
                                TimeUnit.MONTH,
                                RelDataType.PRECISION_NOT_SPECIFIED,
                                SqlParserPos.ZERO)),
                rexBuilder.makeIntervalLiteral(
                        BigDecimal.valueOf(3),
                        new SqlIntervalQualifier(
                                TimeUnit.DAY, 2, TimeUnit.SECOND, 6, SqlParserPos.ZERO)),
                rexBuilder.makeIntervalLiteral(
                        BigDecimal.valueOf(3),
                        new SqlIntervalQualifier(
                                TimeUnit.SECOND, 2, TimeUnit.SECOND, 6, SqlParserPos.ZERO)),
                rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch(10)),
                rexBuilder.makeDateLiteral(new DateString("2000-12-12")),
                rexBuilder.makeTimeLiteral(TimeString.fromMillisOfDay(1234), 3),
                rexBuilder.makeTimeLiteral(TimeString.fromMillisOfDay(123456), 6),
                rexBuilder.makeTimeLiteral(new TimeString("01:01:01.000000001"), 9),
                rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(1234), 3),
                rexBuilder.makeTimestampLiteral(TimestampString.fromMillisSinceEpoch(123456789), 9),
                rexBuilder.makeTimestampLiteral(
                        new TimestampString("0001-01-01 01:01:01.000000001"), 9),
                rexBuilder.makeTimestampLiteral(new TimestampString("2000-12-12 12:30:57.1234"), 4),
                rexBuilder.makeBinaryLiteral(ByteString.EMPTY),
                rexBuilder.makeBinaryLiteral(ByteString.ofBase64("SGVsbG8gV29ybGQh")),
                rexBuilder.makeLiteral(""),
                rexBuilder.makeLiteral("abc"),
                rexBuilder.makeFlag(SqlTrimFunction.Flag.BOTH),
                rexBuilder.makeFlag(TimeUnitRange.DAY),
                rexBuilder.makeLiteral(
                        Arrays.<Object>asList(1, 2L),
                        FACTORY.createStructType(
                                StructKind.PEEK_FIELDS_NO_EXPAND,
                                Arrays.asList(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER),
                                        FACTORY.createSqlType(SqlTypeName.BIGINT)),
                                Arrays.asList("f1", "f2")),
                        false),
                rexBuilder.makeSearchArgumentLiteral(
                        Sarg.of(
                                false,
                                ImmutableRangeSet.of(
                                        Range.closed(
                                                BigDecimal.valueOf(1), BigDecimal.valueOf(10)))),
                        FACTORY.createSqlType(SqlTypeName.INTEGER)),
                rexBuilder.makeSearchArgumentLiteral(
                        Sarg.of(
                                false,
                                ImmutableRangeSet.of(
                                        Range.range(
                                                BigDecimal.valueOf(1),
                                                BoundType.OPEN,
                                                BigDecimal.valueOf(10),
                                                BoundType.CLOSED))),
                        FACTORY.createSqlType(SqlTypeName.INTEGER)),
                rexBuilder.makeSearchArgumentLiteral(
                        Sarg.of(
                                false,
                                TreeRangeSet.create(
                                        Arrays.asList(
                                                Range.closed(
                                                        BigDecimal.valueOf(1),
                                                        BigDecimal.valueOf(1)),
                                                Range.closed(
                                                        BigDecimal.valueOf(3),
                                                        BigDecimal.valueOf(3)),
                                                Range.closed(
                                                        BigDecimal.valueOf(6),
                                                        BigDecimal.valueOf(6))))),
                        FACTORY.createSqlType(SqlTypeName.INTEGER)),
                rexBuilder.makeInputRef(FACTORY.createSqlType(SqlTypeName.BIGINT), 0),
                rexBuilder.makeCorrel(inputType, new CorrelationId("$cor1")),
                rexBuilder.makeFieldAccess(
                        rexBuilder.makeCorrel(inputType, new CorrelationId("$cor2")), "f2", true),
                // cast($1 as smallint)
                rexBuilder.makeCast(
                        FACTORY.createSqlType(SqlTypeName.SMALLINT),
                        rexBuilder.makeInputRef(FACTORY.createSqlType(SqlTypeName.INTEGER), 1)),
                // $1 in (1, 3, 5)
                rexBuilder.makeIn(
                        rexBuilder.makeInputRef(FACTORY.createSqlType(SqlTypeName.INTEGER), 1),
                        Arrays.asList(
                                rexBuilder.makeExactLiteral(new BigDecimal(1)),
                                rexBuilder.makeExactLiteral(new BigDecimal(3)),
                                rexBuilder.makeExactLiteral(new BigDecimal(5)))),
                // null or $1 is null
                rexBuilder.makeCall(
                        SqlStdOperatorTable.OR,
                        rexBuilder.makeNullLiteral(FACTORY.createSqlType(SqlTypeName.INTEGER)),
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.IS_NOT_NULL,
                                rexBuilder.makeInputRef(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER), 1))),
                // $1 >= 10
                rexBuilder.makeCall(
                        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        rexBuilder.makeInputRef(FACTORY.createSqlType(SqlTypeName.INTEGER), 1),
                        rexBuilder.makeExactLiteral(new BigDecimal(10))),
                // hash_code($1)
                rexBuilder.makeCall(
                        FlinkSqlOperatorTable.HASH_CODE,
                        rexBuilder.makeInputRef(FACTORY.createSqlType(SqlTypeName.INTEGER), 1)),
                rexBuilder.makePatternFieldRef(
                        "test", FACTORY.createSqlType(SqlTypeName.INTEGER), 0));
    }
}
