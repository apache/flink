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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.sql.BuiltInSqlOperator;
import org.apache.flink.table.planner.typeutils.SymbolUtil.SerializableSymbol;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableRangeSet.Builder;
import static com.google.common.collect.ImmutableRangeSet.builder;
import static com.google.common.collect.Range.all;
import static com.google.common.collect.Range.atLeast;
import static com.google.common.collect.Range.atMost;
import static com.google.common.collect.Range.greaterThan;
import static com.google.common.collect.Range.lessThan;
import static org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore.IDENTIFIER;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil.loadClass;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_ALPHA;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_BOUND_LOWER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_BOUND_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_BOUND_UPPER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_CATALOG_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_CONTAINS_NULL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_CORREL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_EXPR;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_INPUT_INDEX;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_INTERNAL_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_NULL_AS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_OPERANDS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_RANGES;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_SARG;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_SQL_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_SYMBOL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_SYNTAX;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_SYSTEM_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_VALUE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.KIND_CALL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.KIND_CORREL_VARIABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.KIND_FIELD_ACCESS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.KIND_INPUT_REF;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.KIND_LITERAL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.KIND_PATTERN_INPUT_REF;
import static org.apache.flink.table.planner.typeutils.SymbolUtil.serializableToCalcite;

/**
 * JSON deserializer for {@link RexNode}.
 *
 * @see RexNodeJsonSerializer for the reverse operation
 */
@Internal
final class RexNodeJsonDeserializer extends StdDeserializer<RexNode> {
    private static final long serialVersionUID = 1L;

    RexNodeJsonDeserializer() {
        super(RexNode.class);
    }

    @Override
    public RexNode deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        final JsonNode jsonNode = jsonParser.readValueAsTree();
        final SerdeContext serdeContext = SerdeContext.get(ctx);
        return deserialize(jsonNode, serdeContext);
    }

    private static RexNode deserialize(JsonNode jsonNode, SerdeContext serdeContext)
            throws IOException {
        final String kind = jsonNode.required(FIELD_NAME_KIND).asText();
        switch (kind) {
            case KIND_INPUT_REF:
                return deserializeInputRef(jsonNode, serdeContext);
            case KIND_LITERAL:
                return deserializeLiteral(jsonNode, serdeContext);
            case KIND_FIELD_ACCESS:
                return deserializeFieldAccess(jsonNode, serdeContext);
            case KIND_CORREL_VARIABLE:
                return deserializeCorrelVariable(jsonNode, serdeContext);
            case KIND_PATTERN_INPUT_REF:
                return deserializePatternFieldRef(jsonNode, serdeContext);
            case KIND_CALL:
                return deserializeCall(jsonNode, serdeContext);
            default:
                throw new TableException("Cannot convert to RexNode: " + jsonNode.toPrettyString());
        }
    }

    private static RexNode deserializeInputRef(JsonNode jsonNode, SerdeContext serdeContext) {
        final int inputIndex = jsonNode.required(FIELD_NAME_INPUT_INDEX).intValue();
        final JsonNode logicalTypeNode = jsonNode.required(FIELD_NAME_TYPE);
        final RelDataType fieldType =
                RelDataTypeJsonDeserializer.deserialize(logicalTypeNode, serdeContext);
        return serdeContext.getRexBuilder().makeInputRef(fieldType, inputIndex);
    }

    private static RexNode deserializeLiteral(JsonNode jsonNode, SerdeContext serdeContext) {
        final JsonNode logicalTypeNode = jsonNode.required(FIELD_NAME_TYPE);
        final RelDataType relDataType =
                RelDataTypeJsonDeserializer.deserialize(logicalTypeNode, serdeContext);
        if (jsonNode.has(FIELD_NAME_SARG)) {
            return deserializeSarg(jsonNode.required(FIELD_NAME_SARG), relDataType, serdeContext);
        } else if (jsonNode.has(FIELD_NAME_VALUE)) {
            final Object value =
                    deserializeLiteralValue(jsonNode, relDataType.getSqlTypeName(), serdeContext);
            if (value == null) {
                return serdeContext.getRexBuilder().makeNullLiteral(relDataType);
            }
            return serdeContext.getRexBuilder().makeLiteral(value, relDataType, true);
        } else {
            throw new TableException("Unknown literal: " + jsonNode.toPrettyString());
        }
    }

    @SuppressWarnings({"UnstableApiUsage", "rawtypes", "unchecked"})
    private static RexNode deserializeSarg(
            JsonNode sargNode, RelDataType relDataType, SerdeContext serdeContext) {
        final RexBuilder rexBuilder = serdeContext.getRexBuilder();
        final ArrayNode rangesNode = (ArrayNode) sargNode.required(FIELD_NAME_RANGES);
        final Builder builder = builder();
        for (JsonNode rangeNode : rangesNode) {
            Range range = all();
            if (rangeNode.has(FIELD_NAME_BOUND_LOWER)) {
                final JsonNode lowerNode = rangeNode.required(FIELD_NAME_BOUND_LOWER);
                final Comparable<?> boundValue =
                        (Comparable<?>)
                                deserializeLiteralValue(
                                        lowerNode, relDataType.getSqlTypeName(), serdeContext);
                assert boundValue != null;
                final BoundType boundType =
                        serializableToCalcite(
                                BoundType.class,
                                lowerNode.required(FIELD_NAME_BOUND_TYPE).asText());
                final Range<?> r =
                        boundType == BoundType.OPEN ? greaterThan(boundValue) : atLeast(boundValue);
                range = range.intersection(r);
            }
            if (rangeNode.has(FIELD_NAME_BOUND_UPPER)) {
                final JsonNode upperNode = rangeNode.required(FIELD_NAME_BOUND_UPPER);
                final Comparable<?> boundValue =
                        (Comparable<?>)
                                deserializeLiteralValue(
                                        upperNode, relDataType.getSqlTypeName(), serdeContext);
                assert boundValue != null;
                final BoundType boundType =
                        serializableToCalcite(
                                BoundType.class,
                                upperNode.required(FIELD_NAME_BOUND_TYPE).asText());
                final Range<?> r =
                        boundType == BoundType.OPEN ? lessThan(boundValue) : atMost(boundValue);
                range = range.intersection(r);
            }
            if (range.hasUpperBound() || range.hasLowerBound()) {
                builder.add(range);
            }
        }
        // TODO: Since 1.18.0 nothing is serialized to FIELD_NAME_CONTAINS_NULL.
        // This if condition (should be removed in future) is required for backward compatibility
        // with Flink 1.17.x where FIELD_NAME_CONTAINS_NULL is still used.
        final JsonNode containsNull = sargNode.get(FIELD_NAME_CONTAINS_NULL);
        if (containsNull != null) {
            return rexBuilder.makeSearchArgumentLiteral(
                    Sarg.of(containsNull.booleanValue(), builder.build()), relDataType);
        }
        final RexUnknownAs nullAs =
                serializableToCalcite(
                        RexUnknownAs.class, sargNode.required(FIELD_NAME_NULL_AS).asText());
        return rexBuilder.makeSearchArgumentLiteral(Sarg.of(nullAs, builder.build()), relDataType);
    }

    private static @Nullable Object deserializeLiteralValue(
            JsonNode literalNode, SqlTypeName sqlTypeName, SerdeContext serdeContext) {
        final JsonNode valueNode = literalNode.required(FIELD_NAME_VALUE);
        if (valueNode.isNull()) {
            return null;
        }

        switch (sqlTypeName) {
            case BOOLEAN:
                return valueNode.booleanValue();
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return new BigDecimal(valueNode.asText());
            case DATE:
                return new DateString(valueNode.asText());
            case TIME:
                return new TimeString(valueNode.asText());
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampString(valueNode.asText());
            case BINARY:
            case VARBINARY:
                return ByteString.ofBase64(valueNode.asText());
            case CHAR:
            case VARCHAR:
                return serdeContext.getRexBuilder().makeLiteral(valueNode.asText()).getValue();
            case SYMBOL:
                final JsonNode symbolNode = literalNode.required(FIELD_NAME_SYMBOL);
                final SerializableSymbol symbol =
                        SerializableSymbol.of(symbolNode.asText(), valueNode.asText());
                return serializableToCalcite(symbol);
            default:
                throw new TableException("Unknown literal: " + valueNode);
        }
    }

    private static RexNode deserializeFieldAccess(JsonNode jsonNode, SerdeContext serdeContext)
            throws IOException {
        final String fieldName = jsonNode.required(FIELD_NAME_NAME).asText();
        final JsonNode exprNode = jsonNode.required(FIELD_NAME_EXPR);
        final RexNode refExpr = deserialize(exprNode, serdeContext);
        return serdeContext.getRexBuilder().makeFieldAccess(refExpr, fieldName, true);
    }

    private static RexNode deserializeCorrelVariable(JsonNode jsonNode, SerdeContext serdeContext) {
        final String correl = jsonNode.required(FIELD_NAME_CORREL).asText();
        final JsonNode logicalTypeNode = jsonNode.required(FIELD_NAME_TYPE);
        final RelDataType fieldType =
                RelDataTypeJsonDeserializer.deserialize(logicalTypeNode, serdeContext);
        return serdeContext.getRexBuilder().makeCorrel(fieldType, new CorrelationId(correl));
    }

    private static RexNode deserializePatternFieldRef(
            JsonNode jsonNode, SerdeContext serdeContext) {
        final int inputIndex = jsonNode.required(FIELD_NAME_INPUT_INDEX).intValue();
        final String alpha = jsonNode.required(FIELD_NAME_ALPHA).asText();
        final JsonNode logicalTypeNode = jsonNode.required(FIELD_NAME_TYPE);
        final RelDataType fieldType =
                RelDataTypeJsonDeserializer.deserialize(logicalTypeNode, serdeContext);
        return serdeContext.getRexBuilder().makePatternFieldRef(alpha, fieldType, inputIndex);
    }

    private static RexNode deserializeCall(JsonNode jsonNode, SerdeContext serdeContext)
            throws IOException {
        final SqlOperator operator = deserializeSqlOperator(jsonNode, serdeContext);
        final ArrayNode operandNodes = (ArrayNode) jsonNode.get(FIELD_NAME_OPERANDS);
        final List<RexNode> rexOperands = new ArrayList<>();
        for (JsonNode node : operandNodes) {
            rexOperands.add(deserialize(node, serdeContext));
        }
        final RelDataType callType;
        if (jsonNode.has(FIELD_NAME_TYPE)) {
            final JsonNode typeNode = jsonNode.get(FIELD_NAME_TYPE);
            callType = RelDataTypeJsonDeserializer.deserialize(typeNode, serdeContext);
        } else {
            callType = serdeContext.getRexBuilder().deriveReturnType(operator, rexOperands);
        }
        return serdeContext.getRexBuilder().makeCall(callType, operator, rexOperands);
    }

    // --------------------------------------------------------------------------------------------

    /** Logic shared with {@link AggregateCallJsonDeserializer}. */
    static SqlOperator deserializeSqlOperator(JsonNode jsonNode, SerdeContext serdeContext) {
        final SqlSyntax syntax;
        if (jsonNode.has(FIELD_NAME_SYNTAX)) {
            syntax =
                    serializableToCalcite(
                            SqlSyntax.class, jsonNode.required(FIELD_NAME_SYNTAX).asText());
        } else {
            syntax = SqlSyntax.FUNCTION;
        }

        if (jsonNode.has(FIELD_NAME_INTERNAL_NAME)) {
            return deserializeInternalFunction(
                    jsonNode.required(FIELD_NAME_INTERNAL_NAME).asText(), syntax, serdeContext);
        } else if (jsonNode.has(FIELD_NAME_CATALOG_NAME)) {
            return deserializeCatalogFunction(jsonNode, syntax, serdeContext);
        } else if (jsonNode.has(FIELD_NAME_CLASS)) {
            return deserializeFunctionClass(jsonNode, serdeContext);
        } else if (jsonNode.has(FIELD_NAME_SYSTEM_NAME)) {
            return deserializeSystemFunction(
                    jsonNode.required(FIELD_NAME_SYSTEM_NAME).asText(), syntax, serdeContext);
        } else if (jsonNode.has(FIELD_NAME_SQL_KIND)) {
            return deserializeInternalFunction(
                    syntax, SqlKind.valueOf(jsonNode.get(FIELD_NAME_SQL_KIND).asText()));
        } else {
            throw new TableException("Invalid function call.");
        }
    }

    private static SqlOperator deserializeSystemFunction(
            String systemName, SqlSyntax syntax, SerdeContext serdeContext) {
        // This method covers both temporary system functions and permanent system
        // functions from a module
        final Optional<SqlOperator> systemOperator =
                lookupOptionalSqlOperator(
                        FunctionIdentifier.of(systemName), syntax, serdeContext, true);
        if (systemOperator.isPresent()) {
            return systemOperator.get();
        }
        throw missingSystemFunction(systemName);
    }

    private static SqlOperator deserializeInternalFunction(
            String internalName, SqlSyntax syntax, SerdeContext serdeContext) {
        // Try $FUNC$1
        final Optional<SqlOperator> internalOperator =
                lookupOptionalSqlOperator(
                        FunctionIdentifier.of(internalName), syntax, serdeContext, false);
        if (internalOperator.isPresent()) {
            return internalOperator.get();
        }
        // Try FUNC
        final String publicName = BuiltInSqlOperator.extractNameFromQualifiedName(internalName);
        final Optional<SqlOperator> latestOperator =
                lookupOptionalSqlOperator(
                        FunctionIdentifier.of(publicName), syntax, serdeContext, true);
        if (latestOperator.isPresent()) {
            return latestOperator.get();
        }

        Optional<SqlOperator> sqlStdOperator =
                lookupOptionalSqlStdOperator(publicName, syntax, null);
        if (sqlStdOperator.isPresent()) {
            return sqlStdOperator.get();
        }

        throw new TableException(
                String.format(
                        "Could not resolve internal system function '%s'. "
                                + "This is a bug, please file an issue.",
                        internalName));
    }

    private static SqlOperator deserializeInternalFunction(SqlSyntax syntax, SqlKind sqlKind) {
        final Optional<SqlOperator> stdOperator = lookupOptionalSqlStdOperator("", syntax, sqlKind);
        if (stdOperator.isPresent()) {
            return stdOperator.get();
        }

        throw new TableException(
                String.format(
                        "Could not resolve internal system function '%s'. "
                                + "This is a bug, please file an issue.",
                        sqlKind.name()));
    }

    private static SqlOperator deserializeFunctionClass(
            JsonNode jsonNode, SerdeContext serdeContext) {
        final String className = jsonNode.required(FIELD_NAME_CLASS).asText();
        final Class<?> functionClass = loadClass(className, serdeContext, "function");
        final UserDefinedFunction functionInstance =
                UserDefinedFunctionHelper.instantiateFunction(functionClass);

        final ContextResolvedFunction resolvedFunction;
        // This can never be a system function
        // because we never serialize classes for system functions
        if (jsonNode.has(FIELD_NAME_CATALOG_NAME)) {
            final ObjectIdentifier objectIdentifier =
                    ObjectIdentifierJsonDeserializer.deserialize(
                            jsonNode.required(FIELD_NAME_CATALOG_NAME).asText(), serdeContext);
            resolvedFunction =
                    ContextResolvedFunction.permanent(
                            FunctionIdentifier.of(objectIdentifier), functionInstance);
        } else {
            resolvedFunction = ContextResolvedFunction.anonymous(functionInstance);
        }

        switch (functionInstance.getKind()) {
            case SCALAR:
            case TABLE:
                return BridgingSqlFunction.of(
                        serdeContext.getFlinkContext(),
                        serdeContext.getTypeFactory(),
                        resolvedFunction);
            case AGGREGATE:
                return BridgingSqlAggFunction.of(
                        serdeContext.getFlinkContext(),
                        serdeContext.getTypeFactory(),
                        resolvedFunction);
            default:
                throw new TableException(
                        String.format(
                                "Unsupported anonymous function kind '%s' for class '%s'.",
                                functionInstance.getKind(), className));
        }
    }

    private static SqlOperator deserializeCatalogFunction(
            JsonNode jsonNode, SqlSyntax syntax, SerdeContext serdeContext) {
        final CatalogPlanRestore restoreStrategy =
                serdeContext.getConfiguration().get(PLAN_RESTORE_CATALOG_OBJECTS);
        final FunctionIdentifier identifier =
                FunctionIdentifier.of(
                        ObjectIdentifierJsonDeserializer.deserialize(
                                jsonNode.required(FIELD_NAME_CATALOG_NAME).asText(), serdeContext));

        switch (restoreStrategy) {
            case ALL:
                {
                    final Optional<SqlOperator> lookupOperator =
                            lookupOptionalSqlOperator(identifier, syntax, serdeContext, false);
                    if (lookupOperator.isPresent()) {
                        return lookupOperator.get();
                    } else if (jsonNode.has(FIELD_NAME_CLASS)) {
                        return deserializeFunctionClass(jsonNode, serdeContext);
                    }
                    throw missingFunctionFromCatalog(identifier, false);
                }
            case ALL_ENFORCED:
                {
                    if (jsonNode.has(FIELD_NAME_CLASS)) {
                        return deserializeFunctionClass(jsonNode, serdeContext);
                    }
                    final Optional<SqlOperator> lookupOperator =
                            lookupOptionalSqlOperator(identifier, syntax, serdeContext, false);
                    if (lookupOperator.map(RexNodeJsonDeserializer::isTemporary).orElse(false)) {
                        return lookupOperator.get();
                    }
                    throw lookupDisabled(identifier);
                }
            case IDENTIFIER:
                final Optional<SqlOperator> lookupOperator =
                        lookupOptionalSqlOperator(identifier, syntax, serdeContext, true);
                if (lookupOperator.isPresent()) {
                    return lookupOperator.get();
                } else {
                    throw missingFunctionFromCatalog(identifier, true);
                }
            default:
                throw new TableException("Unsupported restore strategy: " + restoreStrategy);
        }
    }

    private static boolean isTemporary(SqlOperator sqlOperator) {
        if (sqlOperator instanceof BridgingSqlFunction) {
            return ((BridgingSqlFunction) sqlOperator).getResolvedFunction().isTemporary();
        } else if (sqlOperator instanceof BridgingSqlAggFunction) {
            return ((BridgingSqlAggFunction) sqlOperator).getResolvedFunction().isTemporary();
        }
        return false;
    }

    private static Optional<SqlOperator> lookupOptionalSqlOperator(
            FunctionIdentifier identifier,
            SqlSyntax syntax,
            SerdeContext serdeContext,
            boolean throwOnError) {
        final List<SqlOperator> foundOperators = new ArrayList<>();
        try {
            serdeContext
                    .getOperatorTable()
                    .lookupOperatorOverloads(
                            new SqlIdentifier(identifier.toList(), new SqlParserPos(0, 0)),
                            null, // category
                            syntax,
                            foundOperators,
                            SqlNameMatchers.liberal());
            if (foundOperators.size() != 1) {
                return Optional.empty();
            }
            return Optional.of(foundOperators.get(0));
        } catch (Throwable t) {
            if (throwOnError) {
                throw new TableException(
                        String.format("Error during lookup of function '%s'.", identifier), t);
            }
            return Optional.empty();
        }
    }

    private static Optional<SqlOperator> lookupOptionalSqlStdOperator(
            String operatorName, SqlSyntax syntax, @Nullable SqlKind sqlKind) {
        List<SqlOperator> foundOperators = new ArrayList<>();
        // try to find operator from std operator table.
        SqlStdOperatorTable.instance()
                .lookupOperatorOverloads(
                        new SqlIdentifier(operatorName, new SqlParserPos(0, 0)),
                        null, // category
                        syntax,
                        foundOperators,
                        SqlNameMatchers.liberal());
        if (foundOperators.size() == 1) {
            return Optional.of(foundOperators.get(0));
        }
        // in case different operator has the same kind, check with both name and kind.
        return foundOperators.stream()
                .filter(o -> sqlKind != null && o.getKind() == sqlKind)
                .findFirst();
    }

    private static TableException missingSystemFunction(String systemName) {
        return new TableException(
                String.format(
                        "Could not lookup system function '%s'. "
                                + "Make sure it has been registered before because temporary "
                                + "functions are not contained in the persisted plan. "
                                + "If the function was provided by a module, make sure to reloaded "
                                + "all used modules in the correct order.",
                        systemName));
    }

    private static TableException lookupDisabled(FunctionIdentifier identifier) {
        return new TableException(
                String.format(
                        "The persisted plan does not include all required catalog metadata for function '%s'. "
                                + "However, lookup is disabled because option '%s' = '%s'. "
                                + "Either enable the catalog lookup with '%s' = '%s' / '%s' or "
                                + "regenerate the plan with '%s' != '%s'. "
                                + "Make sure the function is not compiled as a temporary function.",
                        identifier.asSummaryString(),
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        CatalogPlanRestore.ALL_ENFORCED.name(),
                        PLAN_RESTORE_CATALOG_OBJECTS.key(),
                        IDENTIFIER.name(),
                        CatalogPlanRestore.ALL.name(),
                        PLAN_COMPILE_CATALOG_OBJECTS.key(),
                        CatalogPlanCompilation.IDENTIFIER.name()));
    }

    private static TableException missingFunctionFromCatalog(
            FunctionIdentifier identifier, boolean forcedLookup) {
        final String initialReason;
        if (forcedLookup) {
            initialReason =
                    String.format(
                            "Cannot resolve function '%s' and catalog lookup is forced because '%s' = '%s'. ",
                            identifier,
                            PLAN_RESTORE_CATALOG_OBJECTS.key(),
                            CatalogPlanRestore.IDENTIFIER);
        } else {
            initialReason =
                    String.format(
                            "Cannot resolve function '%s' and the persisted plan does not include "
                                    + "all required catalog function metadata. ",
                            identifier);
        }
        return new TableException(
                initialReason
                        + String.format(
                                "Make sure a registered catalog contains the function when restoring or "
                                        + "the function is available as a temporary function. "
                                        + "Otherwise regenerate the plan with '%s' != '%s' and make "
                                        + "sure the function was not compiled as a temporary function.",
                                PLAN_COMPILE_CATALOG_OBJECTS.key(),
                                CatalogPlanCompilation.IDENTIFIER.name()));
    }
}
