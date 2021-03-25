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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_ALPHA;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_BOUND_LOWER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_BOUND_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_BOUND_UPPER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_BRIDGING;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_BUILT_IN;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_CLASS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_CONTAINS_NULL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_CORREL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_DISPLAY_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_EXPR;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_FUNCTION_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_INPUT_INDEX;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_INSTANCE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_OPERANDS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_OPERATOR;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_RANGES;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_SARG;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_SYNTAX;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_TYPE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.FIELD_NAME_VALUE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.SQL_KIND_CORREL_VARIABLE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.SQL_KIND_FIELD_ACCESS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.SQL_KIND_INPUT_REF;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.SQL_KIND_LITERAL;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.SQL_KIND_PATTERN_INPUT_REF;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer.SQL_KIND_REX_CALL;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** JSON deserializer for {@link RexNode}. refer to {@link RexNodeJsonSerializer} for serializer. */
public class RexNodeJsonDeserializer extends StdDeserializer<RexNode> {
    private static final long serialVersionUID = 1L;

    public RexNodeJsonDeserializer() {
        super(RexNode.class);
    }

    @Override
    public RexNode deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException, JsonProcessingException {
        JsonNode jsonNode = jsonParser.readValueAsTree();
        return deserializeRexNode(jsonNode, ((FlinkDeserializationContext) ctx));
    }

    private RexNode deserializeRexNode(JsonNode jsonNode, FlinkDeserializationContext ctx)
            throws IOException {
        String kind = jsonNode.get(FIELD_NAME_KIND).asText().toUpperCase();
        switch (kind) {
            case SQL_KIND_INPUT_REF:
                return deserializeInputRef(jsonNode, ctx);
            case SQL_KIND_LITERAL:
                return deserializeLiteral(jsonNode, ctx);
            case SQL_KIND_FIELD_ACCESS:
                return deserializeFieldAccess(jsonNode, ctx);
            case SQL_KIND_CORREL_VARIABLE:
                return deserializeCorrelVariable(jsonNode, ctx);
            case SQL_KIND_REX_CALL:
                return deserializeCall(jsonNode, ctx);
            case SQL_KIND_PATTERN_INPUT_REF:
                return deserializePatternInputRef(jsonNode, ctx);
            default:
                throw new TableException("Cannot convert to RexNode: " + jsonNode.toPrettyString());
        }
    }

    private RexNode deserializeInputRef(JsonNode jsonNode, FlinkDeserializationContext ctx)
            throws JsonProcessingException {
        int inputIndex = jsonNode.get(FIELD_NAME_INPUT_INDEX).intValue();
        JsonNode typeNode = jsonNode.get(FIELD_NAME_TYPE);
        RelDataType fieldType =
                ctx.getObjectMapper().readValue(typeNode.toPrettyString(), RelDataType.class);
        return ctx.getSerdeContext().getRexBuilder().makeInputRef(fieldType, inputIndex);
    }

    private RexNode deserializePatternInputRef(JsonNode jsonNode, FlinkDeserializationContext ctx)
            throws JsonProcessingException {
        int inputIndex = jsonNode.get(FIELD_NAME_INPUT_INDEX).intValue();
        String alpha = jsonNode.get(FIELD_NAME_ALPHA).asText();
        JsonNode typeNode = jsonNode.get(FIELD_NAME_TYPE);
        RelDataType fieldType =
                ctx.getObjectMapper().readValue(typeNode.toPrettyString(), RelDataType.class);
        return ctx.getSerdeContext()
                .getRexBuilder()
                .makePatternFieldRef(alpha, fieldType, inputIndex);
    }

    private RexNode deserializeLiteral(JsonNode jsonNode, FlinkDeserializationContext ctx)
            throws IOException {
        RexBuilder rexBuilder = ctx.getSerdeContext().getRexBuilder();
        JsonNode typeNode = jsonNode.get(FIELD_NAME_TYPE);
        RelDataType literalType =
                ctx.getObjectMapper().readValue(typeNode.toPrettyString(), RelDataType.class);
        if (jsonNode.has(FIELD_NAME_SARG)) {
            Sarg<?> sarg = toSarg(jsonNode.get(FIELD_NAME_SARG), literalType.getSqlTypeName(), ctx);
            return rexBuilder.makeSearchArgumentLiteral(sarg, literalType);
        } else if (jsonNode.has(FIELD_NAME_VALUE)) {
            JsonNode literalNode = jsonNode.get(FIELD_NAME_VALUE);
            if (literalNode.isNull()) {
                return rexBuilder.makeNullLiteral(literalType);
            }
            Object literal = toLiteralValue(jsonNode, literalType.getSqlTypeName(), ctx);
            return rexBuilder.makeLiteral(literal, literalType, true);
        } else {
            throw new TableException("Unknown literal: " + jsonNode.toPrettyString());
        }
    }

    private Object toLiteralValue(
            JsonNode literalNode, SqlTypeName sqlTypeName, FlinkDeserializationContext ctx)
            throws IOException {
        JsonNode valueNode = literalNode.get(FIELD_NAME_VALUE);
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
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
            case REAL:
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
            case TIME_WITH_LOCAL_TIME_ZONE:
                return new TimeString(valueNode.asText());
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampString(valueNode.asText());
            case BINARY:
            case VARBINARY:
                return ByteString.ofBase64(valueNode.asText());
            case CHAR:
            case VARCHAR:
                return ctx.getSerdeContext()
                        .getRexBuilder()
                        .makeLiteral(valueNode.asText())
                        .getValue();
            case SYMBOL:
                JsonNode classNode = literalNode.get(FIELD_NAME_CLASS);
                return getEnum(
                        classNode.asText(),
                        valueNode.asText(),
                        ctx.getSerdeContext().getClassLoader());
            case ROW:
            case MULTISET:
                ArrayNode valuesNode = (ArrayNode) valueNode;
                List<RexNode> list = new ArrayList<>();
                for (int i = 0; i < valuesNode.size(); ++i) {
                    list.add(deserializeRexNode(valuesNode.get(i), ctx));
                }
                return list;
            default:
                throw new TableException("Unknown literal: " + valueNode);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Enum<T>> T getEnum(
            String clazz, String name, ClassLoader classLoader) {
        try {
            Class<T> c = (Class<T>) Class.forName(clazz, true, classLoader);
            return Enum.valueOf(c, name);
        } catch (ClassNotFoundException e) {
            throw new TableException("Unknown class: " + clazz);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked", "UnstableApiUsage"})
    private Sarg<?> toSarg(
            JsonNode jsonNode, SqlTypeName sqlTypeName, FlinkDeserializationContext ctx)
            throws IOException {
        ArrayNode rangesNode = (ArrayNode) jsonNode.get(FIELD_NAME_RANGES);
        com.google.common.collect.ImmutableRangeSet.Builder builder =
                com.google.common.collect.ImmutableRangeSet.builder();
        for (JsonNode rangeNode : rangesNode) {
            com.google.common.collect.Range<?> range = com.google.common.collect.Range.all();
            if (rangeNode.has(FIELD_NAME_BOUND_LOWER)) {
                JsonNode lowerNode = rangeNode.get(FIELD_NAME_BOUND_LOWER);
                Comparable<?> boundValue =
                        checkNotNull((Comparable<?>) toLiteralValue(lowerNode, sqlTypeName, ctx));
                com.google.common.collect.BoundType boundType =
                        com.google.common.collect.BoundType.valueOf(
                                lowerNode.get(FIELD_NAME_BOUND_TYPE).asText().toUpperCase());
                com.google.common.collect.Range r =
                        boundType == com.google.common.collect.BoundType.OPEN
                                ? com.google.common.collect.Range.greaterThan(boundValue)
                                : com.google.common.collect.Range.atLeast(boundValue);
                range = range.intersection(r);
            }
            if (rangeNode.has(FIELD_NAME_BOUND_UPPER)) {
                JsonNode upperNode = rangeNode.get(FIELD_NAME_BOUND_UPPER);
                Comparable<?> boundValue =
                        checkNotNull((Comparable<?>) toLiteralValue(upperNode, sqlTypeName, ctx));
                com.google.common.collect.BoundType boundType =
                        com.google.common.collect.BoundType.valueOf(
                                upperNode.get(FIELD_NAME_BOUND_TYPE).asText().toUpperCase());
                com.google.common.collect.Range r =
                        boundType == com.google.common.collect.BoundType.OPEN
                                ? com.google.common.collect.Range.lessThan(boundValue)
                                : com.google.common.collect.Range.atMost(boundValue);
                range = range.intersection(r);
            }
            if (range.hasUpperBound() || range.hasLowerBound()) {
                builder.add(range);
            }
        }
        boolean containsNull = jsonNode.get(FIELD_NAME_CONTAINS_NULL).booleanValue();
        return Sarg.of(containsNull, builder.build());
    }

    private RexNode deserializeFieldAccess(JsonNode jsonNode, FlinkDeserializationContext ctx)
            throws IOException {
        String fieldName = jsonNode.get(FIELD_NAME_NAME).asText();
        JsonNode exprNode = jsonNode.get(FIELD_NAME_EXPR);
        RexNode refExpr = deserializeRexNode(exprNode, ctx);
        return ctx.getSerdeContext().getRexBuilder().makeFieldAccess(refExpr, fieldName, true);
    }

    private RexNode deserializeCorrelVariable(JsonNode jsonNode, FlinkDeserializationContext ctx)
            throws JsonProcessingException {
        String correl = jsonNode.get(FIELD_NAME_CORREL).asText();
        JsonNode typeNode = jsonNode.get(FIELD_NAME_TYPE);
        RelDataType fieldType =
                ctx.getObjectMapper().readValue(typeNode.toPrettyString(), RelDataType.class);
        return ctx.getSerdeContext()
                .getRexBuilder()
                .makeCorrel(fieldType, new CorrelationId(correl));
    }

    private RexNode deserializeCall(JsonNode jsonNode, FlinkDeserializationContext ctx)
            throws IOException {
        RexBuilder rexBuilder = ctx.getSerdeContext().getRexBuilder();
        SqlOperator operator = toOperator(jsonNode.get(FIELD_NAME_OPERATOR), ctx.getSerdeContext());
        ArrayNode operandNodes = (ArrayNode) jsonNode.get(FIELD_NAME_OPERANDS);
        List<RexNode> rexOperands = new ArrayList<>();
        for (JsonNode node : operandNodes) {
            rexOperands.add(deserializeRexNode(node, ctx));
        }
        final RelDataType callType;
        if (jsonNode.has(FIELD_NAME_TYPE)) {
            JsonNode typeNode = jsonNode.get(FIELD_NAME_TYPE);
            callType =
                    ctx.getObjectMapper().readValue(typeNode.toPrettyString(), RelDataType.class);
        } else {
            callType = rexBuilder.deriveReturnType(operator, rexOperands);
        }
        return rexBuilder.makeCall(callType, operator, rexOperands);
    }

    private SqlOperator toOperator(JsonNode jsonNode, SerdeContext ctx) throws IOException {
        String name = jsonNode.get(FIELD_NAME_NAME).asText();
        SqlKind sqlKind = SqlKind.valueOf(jsonNode.get(FIELD_NAME_KIND).asText());
        SqlSyntax sqlSyntax = SqlSyntax.valueOf(jsonNode.get(FIELD_NAME_SYNTAX).asText());
        List<SqlOperator> operators = new ArrayList<>();
        ctx.getOperatorTable()
                .lookupOperatorOverloads(
                        new SqlIdentifier(name, new SqlParserPos(0, 0)),
                        null, // category
                        sqlSyntax,
                        operators,
                        SqlNameMatchers.liberal());
        for (SqlOperator operator : operators) {
            // in case different operator has the same kind, check with both name and kind.
            if (operator.kind == sqlKind) {
                return operator;
            }
        }

        // try to find operator from std operator table.
        SqlStdOperatorTable.instance()
                .lookupOperatorOverloads(
                        new SqlIdentifier(name, new SqlParserPos(0, 0)),
                        null, // category
                        sqlSyntax,
                        operators,
                        SqlNameMatchers.liberal());
        for (SqlOperator operator : operators) {
            // in case different operator has the same kind, check with both name and kind.
            if (operator.kind == sqlKind) {
                return operator;
            }
        }

        // built-in function
        // TODO supports other module's built-in function
        if (jsonNode.has(FIELD_NAME_BUILT_IN) && jsonNode.get(FIELD_NAME_BUILT_IN).booleanValue()) {
            Optional<FunctionDefinition> function = CoreModule.INSTANCE.getFunctionDefinition(name);
            Preconditions.checkArgument(function.isPresent());
            return BridgingSqlFunction.of(
                    ctx.getFlinkContext(),
                    ctx.getTypeFactory(),
                    FunctionIdentifier.of(name),
                    function.get());
        }

        if (jsonNode.has(FIELD_NAME_FUNCTION_KIND) && jsonNode.has(FIELD_NAME_INSTANCE)) {
            FunctionKind functionKind =
                    FunctionKind.valueOf(
                            jsonNode.get(FIELD_NAME_FUNCTION_KIND).asText().toUpperCase());
            String instanceStr = jsonNode.get(FIELD_NAME_INSTANCE).asText();
            if (functionKind != FunctionKind.SCALAR) {
                throw new TableException("Unknown function kind: " + functionKind);
            }
            if (jsonNode.has(FIELD_NAME_BRIDGING)
                    && jsonNode.get(FIELD_NAME_BRIDGING).booleanValue()) {
                FunctionDefinition function =
                        EncodingUtils.decodeStringToObject(instanceStr, ctx.getClassLoader());
                return BridgingSqlFunction.of(
                        ctx.getFlinkContext(),
                        ctx.getTypeFactory(),
                        FunctionIdentifier.of(name),
                        function);
            } else {
                String displayName = jsonNode.get(FIELD_NAME_DISPLAY_NAME).asText();
                ScalarFunction function =
                        EncodingUtils.decodeStringToObject(instanceStr, ctx.getClassLoader());
                return new ScalarSqlFunction(
                        FunctionIdentifier.of(name),
                        displayName,
                        function,
                        ctx.getTypeFactory(),
                        JavaScalaConversionUtil.toScala(Optional.empty()));
            }
        }
        throw new TableException("Unknown operator: " + jsonNode.toPrettyString());
    }
}
