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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.assertThatJsonContains;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.assertThatJsonDoesNotContain;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.createObjectReader;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.createObjectWriter;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_CLASS;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_CATALOG;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link RexNode} serialization and deserialization. */
@Execution(CONCURRENT)
public class RexNodeJsonSerdeTest {

    private static final FlinkTypeFactory FACTORY = FlinkTypeFactory.INSTANCE();
    private static final String FUNCTION_NAME = "MyFunc";
    private static final FunctionIdentifier FUNCTION_SYS_ID = FunctionIdentifier.of(FUNCTION_NAME);
    private static final FunctionIdentifier FUNCTION_CAT_ID =
            FunctionIdentifier.of(
                    ObjectIdentifier.of(DEFAULT_CATALOG, DEFAULT_DATABASE, FUNCTION_NAME));
    private static final UnresolvedIdentifier UNRESOLVED_FUNCTION_CAT_ID =
            UnresolvedIdentifier.of(FUNCTION_CAT_ID.toList());
    private static final SerializableScalarFunction SER_UDF_IMPL = new SerializableScalarFunction();
    private static final Class<SerializableScalarFunction> SER_UDF_CLASS =
            SerializableScalarFunction.class;
    private static final OtherSerializableScalarFunction SER_UDF_IMPL_OTHER =
            new OtherSerializableScalarFunction();
    private static final Class<OtherSerializableScalarFunction> SER_UDF_CLASS_OTHER =
            OtherSerializableScalarFunction.class;
    private static final NonSerializableScalarFunction NON_SER_UDF_IMPL =
            new NonSerializableScalarFunction(true);
    private static final NonSerializableFunctionDefinition NON_SER_FUNCTION_DEF_IMPL =
            new NonSerializableFunctionDefinition();
    private static final ContextResolvedFunction PERMANENT_FUNCTION =
            ContextResolvedFunction.permanent(FUNCTION_CAT_ID, SER_UDF_IMPL);

    @ParameterizedTest
    @MethodSource("testRexNodeSerde")
    public void testRexNodeSerde(RexNode rexNode) throws IOException {
        final RexNode actual = testJsonRoundTrip(rexNode, RexNode.class);
        assertThat(actual).isEqualTo(rexNode);
    }

    @Test
    public void testInlineFunction() throws IOException {
        final SerdeContext serdeContext = contradictingSerdeContext();

        // Serializable function
        testJsonRoundTrip(
                createFunctionCall(serdeContext, ContextResolvedFunction.anonymous(SER_UDF_IMPL)),
                RexNode.class);

        // Non-serializable function due to fields
        assertThatThrownBy(
                        () ->
                                toJson(
                                        serdeContext,
                                        createFunctionCall(
                                                serdeContext,
                                                ContextResolvedFunction.anonymous(
                                                        NON_SER_UDF_IMPL))))
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "The function's implementation class must not be stateful"));
    }

    @Test
    public void testSystemFunction() throws Throwable {
        final SerdeContext serdeContext = contradictingSerdeContext();

        final ThrowingCallable callable =
                () ->
                        testJsonRoundTrip(
                                serdeContext,
                                createFunctionCall(
                                        serdeContext,
                                        ContextResolvedFunction.permanent(
                                                FUNCTION_SYS_ID, NON_SER_UDF_IMPL)),
                                RexNode.class);

        // Missing function
        assertThatThrownBy(callable)
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "Could not lookup system function '" + FUNCTION_NAME + "'."));

        // Module provided permanent function
        serdeContext
                .getFlinkContext()
                .getModuleManager()
                .loadModule("myModule", FunctionProvidingModule.INSTANCE);
        callable.call();
    }

    @Test
    public void testTemporarySystemFunction() throws Throwable {
        final SerdeContext serdeContext = contradictingSerdeContext();

        final ThrowingCallable callable =
                () ->
                        testJsonRoundTrip(
                                serdeContext,
                                createFunctionCall(
                                        serdeContext,
                                        ContextResolvedFunction.temporary(
                                                FUNCTION_SYS_ID, NON_SER_UDF_IMPL)),
                                RexNode.class);

        // Missing function
        assertThatThrownBy(callable)
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "Could not lookup system function '" + FUNCTION_NAME + "'."));

        // Registered temporary function
        registerTemporaryFunction(serdeContext);
        callable.call();
    }

    @Test
    public void testTemporaryCatalogFunction() throws Throwable {
        final SerdeContext serdeContext = contradictingSerdeContext();

        final ThrowingCallable callable =
                () ->
                        testJsonRoundTrip(
                                serdeContext,
                                createFunctionCall(
                                        serdeContext,
                                        ContextResolvedFunction.temporary(
                                                FUNCTION_CAT_ID, NON_SER_FUNCTION_DEF_IMPL)),
                                RexNode.class);

        // Missing function
        assertThatThrownBy(callable)
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "The persisted plan does not include all required "
                                        + "catalog metadata for function '"
                                        + FUNCTION_CAT_ID.asSummaryString()
                                        + "'."));

        // Registered temporary function
        registerTemporaryFunction(serdeContext);
        callable.call();
    }

    @Test
    public void testUnsupportedLegacyFunction() {
        final SerdeContext serdeContext = contradictingSerdeContext();

        assertThatThrownBy(
                        () ->
                                testJsonRoundTrip(
                                        createFunctionCall(
                                                serdeContext,
                                                UserDefinedFunctionUtils.createScalarSqlFunction(
                                                        FUNCTION_SYS_ID,
                                                        FUNCTION_SYS_ID.toString(),
                                                        SER_UDF_IMPL,
                                                        FACTORY)),
                                        RexNode.class))
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "Functions of the deprecated function stack are not supported."));
    }

    @Nested
    @DisplayName("Test CatalogPlanCompilation == IDENTIFIER")
    class TestCompileIdentifier {

        private final CatalogPlanCompilation compilation = CatalogPlanCompilation.IDENTIFIER;

        @Nested
        @DisplayName("and CatalogPlanRestore == IDENTIFIER")
        class TestRestoreIdentifier {

            private final CatalogPlanRestore restore = CatalogPlanRestore.IDENTIFIER;

            @Test
            void withConstantCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);
                assertThatJsonDoesNotContain(json, FIELD_NAME_CLASS);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual).isEqualTo(PERMANENT_FUNCTION);
            }

            @Test
            void withDroppedCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);

                dropPermanentFunction(ctx);

                assertThatThrownBy(() -> deserialize(ctx, json))
                        .satisfies(
                                anyCauseMatches(
                                        TableException.class,
                                        "Make sure a registered catalog contains the function"));
            }

            @Test
            void withModifiedCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);

                modifyPermanentFunction(ctx);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual)
                        .isEqualTo(
                                ContextResolvedFunction.permanent(
                                        FUNCTION_CAT_ID, SER_UDF_IMPL_OTHER));
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL_ENFORCED")
        class TestRestoreAllEnforced {

            private final CatalogPlanRestore restore = CatalogPlanRestore.ALL_ENFORCED;

            @Test
            void withConstantCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);
                assertThatJsonDoesNotContain(json, FIELD_NAME_CLASS);

                assertThatThrownBy(() -> deserialize(ctx, json))
                        .satisfies(
                                anyCauseMatches(
                                        TableException.class,
                                        "plan does not include all required catalog metadata"));
            }

            @Test
            void withShadowingTemporaryFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);

                // The temporary function can be used as a replacement for the disabled catalog
                // lookup
                registerTemporaryFunction(ctx);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual)
                        .isEqualTo(
                                ContextResolvedFunction.temporary(
                                        FUNCTION_CAT_ID, NON_SER_FUNCTION_DEF_IMPL));
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL")
        class TestRestoreAll {

            private final CatalogPlanRestore restore = CatalogPlanRestore.ALL;

            @Test
            void withConstantCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);
                assertThatJsonDoesNotContain(json, FIELD_NAME_CLASS);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual).isEqualTo(PERMANENT_FUNCTION);
            }

            @Test
            void withShadowingTemporaryFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);

                // The temporary function can be used as a replacement for the disabled catalog
                // lookup
                registerTemporaryFunction(ctx);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual)
                        .isEqualTo(
                                ContextResolvedFunction.temporary(
                                        FUNCTION_CAT_ID, NON_SER_FUNCTION_DEF_IMPL));
            }

            @Test
            void withModifiedCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);

                // Can replace the original function
                modifyPermanentFunction(ctx);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual)
                        .isEqualTo(
                                ContextResolvedFunction.permanent(
                                        FUNCTION_CAT_ID, SER_UDF_IMPL_OTHER));
            }
        }
    }

    @Nested
    @DisplayName("Test CatalogPlanCompilation == ALL")
    class TestCompileAll {

        private final CatalogPlanCompilation compilation = CatalogPlanCompilation.ALL;

        @Nested
        @DisplayName("and CatalogPlanRestore == IDENTIFIER")
        class TestRestoreIdentifier {

            private final CatalogPlanRestore restore = CatalogPlanRestore.IDENTIFIER;

            @Test
            void withConstantCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);
                assertThatJsonContains(json, FIELD_NAME_CLASS);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual).isEqualTo(PERMANENT_FUNCTION);
            }

            @Test
            void withDroppedCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);

                dropPermanentFunction(ctx);

                assertThatThrownBy(() -> deserialize(ctx, json))
                        .satisfies(
                                anyCauseMatches(
                                        TableException.class,
                                        "Make sure a registered catalog contains the function"));
            }

            @Test
            void withShadowingTemporaryFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);

                // The temporary function can be used as a replacement for the disabled catalog
                // lookup
                registerTemporaryFunction(ctx);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual)
                        .isEqualTo(
                                ContextResolvedFunction.temporary(
                                        FUNCTION_CAT_ID, NON_SER_FUNCTION_DEF_IMPL));
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL_ENFORCED")
        class TestRestoreAllEnforced {

            private final CatalogPlanRestore restore = CatalogPlanRestore.ALL_ENFORCED;

            @Test
            void withConstantCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                // Dropping has no effect
                dropPermanentFunction(ctx);

                // Also shadowing the original one has no effect
                registerTemporaryFunction(ctx);

                final JsonNode json = serializePermanentFunction(ctx);
                assertThatJsonContains(json, FIELD_NAME_CLASS);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual)
                        .isEqualTo(
                                ContextResolvedFunction.permanent(FUNCTION_CAT_ID, SER_UDF_IMPL));
            }
        }

        @Nested
        @DisplayName("and CatalogPlanRestore == ALL")
        class TestRestoreAll {

            private final CatalogPlanRestore restore = CatalogPlanRestore.ALL;

            @Test
            void withConstantCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);
                assertThatJsonContains(json, FIELD_NAME_CLASS);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                // The serde is symmetric, because the function is still present in the catalog
                assertThat(actual).isEqualTo(PERMANENT_FUNCTION);
            }

            @Test
            void withDroppedCatalogFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);
                assertThatJsonContains(json, FIELD_NAME_CLASS);

                dropPermanentFunction(ctx);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                assertThat(actual)
                        .isEqualTo(
                                ContextResolvedFunction.permanent(FUNCTION_CAT_ID, SER_UDF_IMPL));
            }

            @Test
            void withShadowingTemporaryFunction() throws Exception {
                final SerdeContext ctx = serdeContextWithPermanentFunction(compilation, restore);

                final JsonNode json = serializePermanentFunction(ctx);
                assertThatJsonContains(json, FIELD_NAME_CLASS);

                registerTemporaryFunction(ctx);

                final ContextResolvedFunction actual = deserialize(ctx, json);
                // The serde is not symmetric, the function is temporary after restore
                assertThat(actual)
                        .isEqualTo(
                                ContextResolvedFunction.temporary(
                                        FUNCTION_CAT_ID, NON_SER_FUNCTION_DEF_IMPL));
            }
        }
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

    // --------------------------------------------------------------------------------------------
    // Helper methods / classes
    // --------------------------------------------------------------------------------------------

    private static RexNode createFunctionCall(
            SerdeContext serdeContext, ContextResolvedFunction resolvedFunction) {
        final BridgingSqlFunction nonSerializableFunction =
                BridgingSqlFunction.of(
                        serdeContext.getFlinkContext(),
                        serdeContext.getTypeFactory(),
                        resolvedFunction);
        return createFunctionCall(serdeContext, nonSerializableFunction);
    }

    private static RexNode createFunctionCall(SerdeContext serdeContext, SqlFunction sqlFunction) {
        return serdeContext
                .getRexBuilder()
                .makeCall(
                        sqlFunction,
                        serdeContext
                                .getRexBuilder()
                                .makeLiteral(
                                        12,
                                        serdeContext
                                                .getTypeFactory()
                                                .createSqlType(SqlTypeName.INTEGER),
                                        false));
    }

    private static SerdeContext serdeContext(
            CatalogPlanCompilation planCompilationOption, CatalogPlanRestore planRestoreOption) {
        final Configuration configuration = new Configuration();
        configuration.set(TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS, planRestoreOption);
        configuration.set(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS, planCompilationOption);

        return JsonSerdeTestUtil.configuredSerdeContext(configuration);
    }

    private static SerdeContext serdeContextWithPermanentFunction(
            CatalogPlanCompilation planCompilationOption, CatalogPlanRestore planRestoreOption) {
        final SerdeContext serdeContext = serdeContext(planCompilationOption, planRestoreOption);
        serdeContext
                .getFlinkContext()
                .getFunctionCatalog()
                .registerCatalogFunction(UNRESOLVED_FUNCTION_CAT_ID, SER_UDF_CLASS, false);
        return serdeContext;
    }

    private static void dropPermanentFunction(SerdeContext serdeContext) {
        serdeContext
                .getFlinkContext()
                .getFunctionCatalog()
                .dropCatalogFunction(UNRESOLVED_FUNCTION_CAT_ID, false);
    }

    private static void modifyPermanentFunction(SerdeContext serdeContext) {
        dropPermanentFunction(serdeContext);
        serdeContext
                .getFlinkContext()
                .getFunctionCatalog()
                .registerCatalogFunction(UNRESOLVED_FUNCTION_CAT_ID, SER_UDF_CLASS_OTHER, false);
    }

    private static void registerTemporaryFunction(SerdeContext serdeContext) {
        serdeContext
                .getFlinkContext()
                .getFunctionCatalog()
                .registerTemporaryCatalogFunction(
                        UNRESOLVED_FUNCTION_CAT_ID, NON_SER_FUNCTION_DEF_IMPL, false);
    }

    private JsonNode serializePermanentFunction(SerdeContext serdeContext) throws Exception {
        final byte[] actualSerialized =
                createObjectWriter(serdeContext)
                        .writeValueAsBytes(createFunctionCall(serdeContext, PERMANENT_FUNCTION));
        return createObjectReader(serdeContext).readTree(actualSerialized);
    }

    private ContextResolvedFunction deserialize(SerdeContext serdeContext, JsonNode node)
            throws IOException {
        final RexNode actualDeserialized =
                createObjectReader(serdeContext).readValue(node, RexNode.class);
        return ((BridgingSqlFunction) ((RexCall) actualDeserialized).getOperator())
                .getResolvedFunction();
    }

    private static SerdeContext contradictingSerdeContext() {
        // these contradicting options should not have an impact on temporary or anonymous functions
        return serdeContext(CatalogPlanCompilation.IDENTIFIER, CatalogPlanRestore.ALL_ENFORCED);
    }

    private static class FunctionProvidingModule implements Module {
        private static final FunctionProvidingModule INSTANCE = new FunctionProvidingModule();

        @Override
        public Set<String> listFunctions() {
            return Collections.singleton(FUNCTION_NAME);
        }

        @Override
        public Optional<FunctionDefinition> getFunctionDefinition(String name) {
            if (name.equalsIgnoreCase(FUNCTION_NAME)) {
                return Optional.of(NON_SER_FUNCTION_DEF_IMPL);
            }
            return Optional.empty();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test functions
    // --------------------------------------------------------------------------------------------

    /** Serializable function. */
    public static class SerializableScalarFunction extends ScalarFunction {

        @SuppressWarnings("unused")
        public String eval(Integer i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SerializableScalarFunction;
        }
    }

    /** Serializable function for modification. */
    public static class OtherSerializableScalarFunction extends ScalarFunction {

        @SuppressWarnings("unused")
        public String eval(Integer i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof OtherSerializableScalarFunction;
        }
    }

    /** Non-serializable function. */
    public static class NonSerializableScalarFunction extends ScalarFunction {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final boolean flag;

        public NonSerializableScalarFunction(boolean flag) {
            this.flag = flag;
        }

        @SuppressWarnings("unused")
        public String eval(Integer i) {
            throw new UnsupportedOperationException();
        }
    }

    /** Non-serializable function definition. */
    public static class NonSerializableFunctionDefinition implements FunctionDefinition {

        @Override
        public FunctionKind getKind() {
            return FunctionKind.SCALAR;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .typedArguments(DataTypes.INT())
                    .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING()))
                    .build();
        }
    }
}
