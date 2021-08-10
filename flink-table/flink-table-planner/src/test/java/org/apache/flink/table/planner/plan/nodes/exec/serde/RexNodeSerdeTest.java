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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/** Tests for serialization/deserialization of {@link RexNode}. */
@RunWith(Parameterized.class)
public class RexNodeSerdeTest {
    private static final FlinkTypeFactory FACTORY = FlinkTypeFactory.INSTANCE();

    @SuppressWarnings({"rawtypes", "unchecked", "UnstableApiUsage"})
    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        TableConfig tableConfig = TableConfig.getDefault();
        CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(Thread.currentThread().getContextClassLoader())
                        .config(tableConfig.getConfiguration())
                        .defaultCatalog("default_catalog", new GenericInMemoryCatalog("default_db"))
                        .build();
        FlinkContext flinkContext =
                new FlinkContextImpl(
                        false,
                        tableConfig,
                        new FunctionCatalog(tableConfig, catalogManager, new ModuleManager()),
                        catalogManager,
                        null); // toRexFactory
        RexBuilder rexBuilder = new RexBuilder(FACTORY);
        RelDataType inputType =
                FACTORY.createStructType(
                        StructKind.PEEK_FIELDS,
                        Arrays.asList(
                                FACTORY.createSqlType(SqlTypeName.INTEGER),
                                FACTORY.createSqlType(SqlTypeName.BIGINT),
                                FACTORY.createStructType(
                                        StructKind.PEEK_FIELDS,
                                        Arrays.asList(
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR),
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR)),
                                        Arrays.asList("n1", "n2"))),
                        Arrays.asList("f1", "f2", "f3"));

        Random random = new Random();
        List<RexNode> rexNodes =
                Arrays.asList(
                        rexBuilder.makeNullLiteral(FACTORY.createSqlType(SqlTypeName.VARCHAR)),
                        rexBuilder.makeLiteral(random.nextBoolean()),
                        rexBuilder.makeExactLiteral(
                                new BigDecimal((byte) random.nextInt()),
                                FACTORY.createSqlType(SqlTypeName.TINYINT)),
                        rexBuilder.makeExactLiteral(
                                new BigDecimal((short) random.nextInt()),
                                FACTORY.createSqlType(SqlTypeName.SMALLINT)),
                        rexBuilder.makeExactLiteral(
                                new BigDecimal(random.nextInt()),
                                FACTORY.createSqlType(SqlTypeName.INTEGER)),
                        rexBuilder.makeExactLiteral(
                                new BigDecimal(random.nextLong()),
                                FACTORY.createSqlType(SqlTypeName.BIGINT)),
                        rexBuilder.makeExactLiteral(
                                BigDecimal.valueOf(random.nextDouble()),
                                FACTORY.createSqlType(SqlTypeName.DOUBLE)),
                        rexBuilder.makeApproxLiteral(
                                BigDecimal.valueOf(random.nextFloat()),
                                FACTORY.createSqlType(SqlTypeName.FLOAT)),
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(random.nextDouble())),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.YEAR, TimeUnit.YEAR, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(100),
                                new SqlIntervalQualifier(
                                        TimeUnit.YEAR, TimeUnit.YEAR, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.MONTH, TimeUnit.MONTH, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.MONTH, TimeUnit.MONTH, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.HOUR, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.HOUR, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.HOUR, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.HOUR, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.MINUTE, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.MINUTE, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.MINUTE, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.MINUTE, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                new SqlIntervalQualifier(
                                        TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        rexBuilder.makeIntervalLiteral(
                                BigDecimal.valueOf(3),
                                new SqlIntervalQualifier(
                                        TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch(10)),
                        rexBuilder.makeDateLiteral(new DateString("2000-12-12")),
                        rexBuilder.makeTimeLiteral(TimeString.fromMillisOfDay(1234), 3),
                        rexBuilder.makeTimeLiteral(TimeString.fromMillisOfDay(123456), 6),
                        rexBuilder.makeTimeLiteral(new TimeString("01:01:01.000000001"), 9),
                        rexBuilder.makeTimeWithLocalTimeZoneLiteral(
                                TimeString.fromMillisOfDay(1234), 3),
                        rexBuilder.makeTimestampLiteral(
                                TimestampString.fromMillisSinceEpoch(1234), 3),
                        rexBuilder.makeTimestampLiteral(
                                TimestampString.fromMillisSinceEpoch(123456789), 9),
                        rexBuilder.makeTimestampLiteral(
                                new TimestampString("0001-01-01 01:01:01.000000001"), 9),
                        rexBuilder.makeTimestampLiteral(
                                new TimestampString("2000-12-12 12:30:57.1234"), 4),
                        rexBuilder.makeBinaryLiteral(ByteString.EMPTY),
                        rexBuilder.makeBinaryLiteral(
                                ByteString.ofBase64(EncodingUtils.encodeObjectToString("abc"))),
                        rexBuilder.makeLiteral(""),
                        rexBuilder.makeLiteral("abc"),
                        rexBuilder.makeFlag(SqlTrimFunction.Flag.BOTH),
                        rexBuilder.makeFlag(TimeUnitRange.DAY),
                        rexBuilder.makeLiteral(
                                Arrays.<Object>asList(1, 2L),
                                FACTORY.createStructType(
                                        Arrays.asList(
                                                FACTORY.createSqlType(SqlTypeName.INTEGER),
                                                FACTORY.createSqlType(SqlTypeName.BIGINT)),
                                        Arrays.asList("f1", "f2")),
                                false),
                        rexBuilder.makeSearchArgumentLiteral(
                                Sarg.of(
                                        false,
                                        com.google.common.collect.ImmutableRangeSet.<Comparable>of(
                                                com.google.common.collect.Range.closed(
                                                        BigDecimal.valueOf(1),
                                                        BigDecimal.valueOf(10)))),
                                FACTORY.createSqlType(SqlTypeName.INTEGER)),
                        rexBuilder.makeSearchArgumentLiteral(
                                Sarg.of(
                                        false,
                                        com.google.common.collect.ImmutableRangeSet.<Comparable>of(
                                                com.google.common.collect.Range.range(
                                                        BigDecimal.valueOf(1),
                                                        com.google.common.collect.BoundType.OPEN,
                                                        BigDecimal.valueOf(10),
                                                        com.google.common.collect.BoundType
                                                                .CLOSED))),
                                FACTORY.createSqlType(SqlTypeName.INTEGER)),
                        rexBuilder.makeSearchArgumentLiteral(
                                Sarg.of(
                                        false,
                                        com.google.common.collect.TreeRangeSet.<Comparable>create(
                                                Arrays.asList(
                                                        com.google.common.collect.Range.closed(
                                                                BigDecimal.valueOf(1),
                                                                BigDecimal.valueOf(1)),
                                                        com.google.common.collect.Range.closed(
                                                                BigDecimal.valueOf(3),
                                                                BigDecimal.valueOf(3)),
                                                        com.google.common.collect.Range.closed(
                                                                BigDecimal.valueOf(6),
                                                                BigDecimal.valueOf(6))))),
                                FACTORY.createSqlType(SqlTypeName.INTEGER)),
                        rexBuilder.makeInputRef(FACTORY.createSqlType(SqlTypeName.BIGINT), 0),
                        rexBuilder.makeCorrel(inputType, new CorrelationId("$cor1")),
                        rexBuilder.makeFieldAccess(
                                rexBuilder.makeCorrel(inputType, new CorrelationId("$cor2")),
                                "f2",
                                true),
                        // cast($1 as smallint)
                        rexBuilder.makeCast(
                                FACTORY.createSqlType(SqlTypeName.SMALLINT),
                                rexBuilder.makeInputRef(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER), 1)),
                        // $1 in (1, 3, 5)
                        rexBuilder.makeIn(
                                rexBuilder.makeInputRef(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER), 1),
                                Arrays.asList(
                                        rexBuilder.makeExactLiteral(new BigDecimal(1)),
                                        rexBuilder.makeExactLiteral(new BigDecimal(3)),
                                        rexBuilder.makeExactLiteral(new BigDecimal(5)))),
                        // null or $1 is null
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.OR,
                                rexBuilder.makeNullLiteral(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER)),
                                rexBuilder.makeCall(
                                        SqlStdOperatorTable.IS_NOT_NULL,
                                        rexBuilder.makeInputRef(
                                                FACTORY.createSqlType(SqlTypeName.INTEGER), 1))),
                        // $1 >= 10
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                rexBuilder.makeInputRef(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER), 1),
                                rexBuilder.makeExactLiteral(new BigDecimal(10))),
                        // hash_code($1)
                        rexBuilder.makeCall(
                                FlinkSqlOperatorTable.HASH_CODE,
                                rexBuilder.makeInputRef(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER), 1)),
                        // MyUdf($0)
                        rexBuilder.makeCall(
                                new ScalarSqlFunction(
                                        FunctionIdentifier.of("MyUdf"),
                                        "MyUdf",
                                        new ScalarFunc1(),
                                        FACTORY,
                                        JavaScalaConversionUtil.toScala(Optional.empty())),
                                rexBuilder.makeInputRef(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER), 0)),
                        // MyUdf($0)
                        rexBuilder.makeCall(
                                BridgingSqlFunction.of(
                                        flinkContext,
                                        FACTORY,
                                        null, // identifier
                                        new ScalarFunc1()),
                                rexBuilder.makeInputRef(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER), 0)),
                        // type_of($0)
                        rexBuilder.makeCall(
                                BridgingSqlFunction.of(
                                        flinkContext,
                                        FACTORY,
                                        FunctionIdentifier.of(
                                                BuiltInFunctionDefinitions.TYPE_OF.getName()),
                                        BuiltInFunctionDefinitions.TYPE_OF),
                                rexBuilder.makeInputRef(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER), 0)),
                        rexBuilder.makePatternFieldRef(
                                "test", FACTORY.createSqlType(SqlTypeName.INTEGER), 0));
        return rexNodes.stream().map(n -> new Object[] {n, flinkContext}).toArray(Object[][]::new);
    }

    @Parameterized.Parameter public RexNode rexNode;

    @Parameterized.Parameter(1)
    public FlinkContext flinkContext;

    @Test
    public void testRexNodeSerde() throws Exception {
        SerdeContext serdeCtx =
                new SerdeContext(
                        flinkContext,
                        Thread.currentThread().getContextClassLoader(),
                        FACTORY,
                        FlinkSqlOperatorTable.instance());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        SimpleModule module = new SimpleModule();

        module.addSerializer(new RexNodeJsonSerializer());
        module.addSerializer(new RelDataTypeJsonSerializer());
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        module.addDeserializer(RelDataType.class, new RelDataTypeJsonDeserializer());
        mapper.registerModule(module);
        StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(rexNode);
        }
        String json = writer.toString();
        RexNode actual = mapper.readValue(json, RexNode.class);
        assertEquals(rexNode, actual);
    }

    /** Testing ScalarFunction. */
    public static class ScalarFunc1 extends ScalarFunction {
        public int eval(int i) {
            return i + 1;
        }
    }
}
