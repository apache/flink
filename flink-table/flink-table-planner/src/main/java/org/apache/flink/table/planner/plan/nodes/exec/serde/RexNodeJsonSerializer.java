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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.sql.BuiltInSqlOperator;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.typeutils.SymbolUtil.SerializableSymbol;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import com.google.common.collect.Range;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import java.io.IOException;
import java.math.BigDecimal;

import static org.apache.calcite.sql.type.SqlTypeName.SARG;
import static org.apache.flink.table.functions.UserDefinedFunctionHelper.isClassNameSerializable;
import static org.apache.flink.table.planner.typeutils.SymbolUtil.calciteToSerializable;

/**
 * JSON serializer for {@link RexNode}.
 *
 * @see RexNodeJsonDeserializer for the reverse operation
 */
@Internal
final class RexNodeJsonSerializer extends StdSerializer<RexNode> {
    private static final long serialVersionUID = 1L;

    // Common fields
    static final String FIELD_NAME_KIND = "kind";
    static final String FIELD_NAME_VALUE = "value";
    static final String FIELD_NAME_TYPE = "type";
    static final String FIELD_NAME_NAME = "name";

    // INPUT_REF
    static final String KIND_INPUT_REF = "INPUT_REF";
    static final String FIELD_NAME_INPUT_INDEX = "inputIndex";

    // LITERAL
    static final String KIND_LITERAL = "LITERAL";
    // Sarg fields and values
    static final String FIELD_NAME_SARG = "sarg";
    static final String FIELD_NAME_RANGES = "ranges";
    static final String FIELD_NAME_BOUND_LOWER = "lower";
    static final String FIELD_NAME_BOUND_UPPER = "upper";
    static final String FIELD_NAME_BOUND_TYPE = "boundType";

    static final String FIELD_NAME_CONTAINS_NULL = "containsNull";
    static final String FIELD_NAME_NULL_AS = "nullAs";
    // Symbol fields
    static final String FIELD_NAME_SYMBOL = "symbol";

    // FIELD_ACCESS
    static final String KIND_FIELD_ACCESS = "FIELD_ACCESS";
    static final String FIELD_NAME_EXPR = "expr";

    // CORREL_VARIABLE
    static final String KIND_CORREL_VARIABLE = "CORREL_VARIABLE";
    static final String FIELD_NAME_CORREL = "correl";

    // PATTERN_INPUT_REF
    static final String KIND_PATTERN_INPUT_REF = "PATTERN_INPUT_REF";
    static final String FIELD_NAME_ALPHA = "alpha";

    // CALL
    static final String KIND_CALL = "CALL";
    static final String FIELD_NAME_OPERANDS = "operands";
    static final String FIELD_NAME_INTERNAL_NAME = "internalName";
    static final String FIELD_NAME_SYSTEM_NAME = "systemName";
    static final String FIELD_NAME_CATALOG_NAME = "catalogName";
    static final String FIELD_NAME_SYNTAX = "syntax";
    static final String FIELD_NAME_SQL_KIND = "sqlKind";
    static final String FIELD_NAME_CLASS = "class";

    RexNodeJsonSerializer() {
        super(RexNode.class);
    }

    @Override
    public void serialize(
            RexNode rexNode, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        final ReadableConfig config = SerdeContext.get(serializerProvider).getConfiguration();
        final CatalogPlanCompilation compilationStrategy =
                config.get(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS);

        switch (rexNode.getKind()) {
            case INPUT_REF:
            case TABLE_INPUT_REF:
                serializeInputRef((RexInputRef) rexNode, jsonGenerator, serializerProvider);
                break;
            case LITERAL:
                serializeLiteral((RexLiteral) rexNode, jsonGenerator, serializerProvider);
                break;
            case FIELD_ACCESS:
                serializeFieldAccess((RexFieldAccess) rexNode, jsonGenerator, serializerProvider);
                break;
            case CORREL_VARIABLE:
                serializeCorrelVariable(
                        (RexCorrelVariable) rexNode, jsonGenerator, serializerProvider);
                break;
            case PATTERN_INPUT_REF:
                serializePatternFieldRef(
                        (RexPatternFieldRef) rexNode, jsonGenerator, serializerProvider);
                break;
            default:
                if (rexNode instanceof RexCall) {
                    serializeCall(
                            (RexCall) rexNode,
                            jsonGenerator,
                            serializerProvider,
                            compilationStrategy);
                } else {
                    throw new TableException("Unknown RexNode: " + rexNode);
                }
        }
    }

    private static void serializePatternFieldRef(
            RexPatternFieldRef inputRef, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, KIND_PATTERN_INPUT_REF);
        gen.writeStringField(FIELD_NAME_ALPHA, inputRef.getAlpha());
        gen.writeNumberField(FIELD_NAME_INPUT_INDEX, inputRef.getIndex());
        serializerProvider.defaultSerializeField(FIELD_NAME_TYPE, inputRef.getType(), gen);
        gen.writeEndObject();
    }

    private static void serializeInputRef(
            RexInputRef inputRef, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, KIND_INPUT_REF);
        gen.writeNumberField(FIELD_NAME_INPUT_INDEX, inputRef.getIndex());
        serializerProvider.defaultSerializeField(FIELD_NAME_TYPE, inputRef.getType(), gen);
        gen.writeEndObject();
    }

    private static void serializeLiteral(
            RexLiteral literal, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, KIND_LITERAL);
        final Comparable<?> value = literal.getValueAs(Comparable.class);
        if (literal.getTypeName() == SARG) {
            serializeSargValue((Sarg<?>) value, literal.getType().getSqlTypeName(), gen);
        } else {
            serializeLiteralValue(value, literal.getType().getSqlTypeName(), gen);
        }
        serializerProvider.defaultSerializeField(FIELD_NAME_TYPE, literal.getType(), gen);
        gen.writeEndObject();
    }

    private static void serializeLiteralValue(
            Comparable<?> value, SqlTypeName literalTypeName, JsonGenerator gen)
            throws IOException {
        if (value == null) {
            gen.writeNullField(FIELD_NAME_VALUE);
            return;
        }

        switch (literalTypeName) {
            case BOOLEAN:
                gen.writeBooleanField(FIELD_NAME_VALUE, (Boolean) value);
                break;
            case TINYINT:
                gen.writeNumberField(FIELD_NAME_VALUE, ((BigDecimal) value).byteValue());
                break;
            case SMALLINT:
                gen.writeNumberField(FIELD_NAME_VALUE, ((BigDecimal) value).shortValue());
                break;
            case INTEGER:
                gen.writeNumberField(FIELD_NAME_VALUE, ((BigDecimal) value).intValue());
                break;
            case BIGINT:
                gen.writeNumberField(FIELD_NAME_VALUE, ((BigDecimal) value).longValue());
                break;
            case DOUBLE:
            case FLOAT:
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
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                gen.writeStringField(FIELD_NAME_VALUE, value.toString());
                break;
            case BINARY:
            case VARBINARY:
                gen.writeStringField(FIELD_NAME_VALUE, ((ByteString) value).toBase64String());
                break;
            case CHAR:
            case VARCHAR:
                gen.writeStringField(FIELD_NAME_VALUE, ((NlsString) value).getValue());
                break;
            case SYMBOL:
                final SerializableSymbol symbol = calciteToSerializable((Enum<?>) value);
                gen.writeStringField(FIELD_NAME_SYMBOL, symbol.getKind());
                gen.writeStringField(FIELD_NAME_VALUE, symbol.getValue());
                break;
            default:
                throw new TableException(
                        String.format(
                                "Literal type '%s' is not supported for serializing value '%s'.",
                                literalTypeName, value));
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private static void serializeSargValue(
            Sarg<?> value, SqlTypeName sqlTypeName, JsonGenerator gen) throws IOException {
        gen.writeFieldName(FIELD_NAME_SARG);
        gen.writeStartObject();
        gen.writeFieldName(FIELD_NAME_RANGES);
        gen.writeStartArray();
        for (Range<?> range : value.rangeSet.asRanges()) {
            gen.writeStartObject();
            if (range.hasLowerBound()) {
                gen.writeFieldName(FIELD_NAME_BOUND_LOWER);
                gen.writeStartObject();
                serializeLiteralValue(range.lowerEndpoint(), sqlTypeName, gen);
                final SerializableSymbol symbol = calciteToSerializable(range.lowerBoundType());
                gen.writeStringField(FIELD_NAME_BOUND_TYPE, symbol.getValue());
                gen.writeEndObject();
            }
            if (range.hasUpperBound()) {
                gen.writeFieldName(FIELD_NAME_BOUND_UPPER);
                gen.writeStartObject();
                serializeLiteralValue(range.upperEndpoint(), sqlTypeName, gen);
                final SerializableSymbol symbol = calciteToSerializable(range.upperBoundType());
                gen.writeStringField(FIELD_NAME_BOUND_TYPE, symbol.getValue());
                gen.writeEndObject();
            }
            gen.writeEndObject();
        }
        gen.writeEndArray();
        final SerializableSymbol symbol = calciteToSerializable(value.nullAs);
        gen.writeStringField(FIELD_NAME_NULL_AS, symbol.getValue());
        gen.writeEndObject();
    }

    private static void serializeFieldAccess(
            RexFieldAccess fieldAccess, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, KIND_FIELD_ACCESS);
        gen.writeStringField(FIELD_NAME_NAME, fieldAccess.getField().getName());
        serializerProvider.defaultSerializeField(
                FIELD_NAME_EXPR, fieldAccess.getReferenceExpr(), gen);
        gen.writeEndObject();
    }

    private static void serializeCorrelVariable(
            RexCorrelVariable variable, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, KIND_CORREL_VARIABLE);
        gen.writeStringField(FIELD_NAME_CORREL, variable.getName());
        serializerProvider.defaultSerializeField(FIELD_NAME_TYPE, variable.getType(), gen);
        gen.writeEndObject();
    }

    private static void serializeCall(
            RexCall call,
            JsonGenerator gen,
            SerializerProvider serializerProvider,
            CatalogPlanCompilation compilationStrategy)
            throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, KIND_CALL);
        serializeSqlOperator(
                call.getOperator(),
                gen,
                serializerProvider,
                compilationStrategy == CatalogPlanCompilation.ALL);
        gen.writeFieldName(FIELD_NAME_OPERANDS);
        gen.writeStartArray();
        for (RexNode operand : call.getOperands()) {
            serializerProvider.defaultSerializeValue(operand, gen);
        }
        gen.writeEndArray();
        serializerProvider.defaultSerializeField(FIELD_NAME_TYPE, call.getType(), gen);
        gen.writeEndObject();
    }

    // --------------------------------------------------------------------------------------------

    /** Logic shared with {@link AggregateCallJsonSerializer}. */
    static void serializeSqlOperator(
            SqlOperator operator,
            JsonGenerator gen,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        if (operator.getSyntax() != SqlSyntax.FUNCTION) {
            gen.writeStringField(
                    FIELD_NAME_SYNTAX, calciteToSerializable(operator.getSyntax()).getValue());
        }
        if (operator instanceof BridgingSqlFunction) {
            final BridgingSqlFunction function = (BridgingSqlFunction) operator;
            serializeBridgingSqlFunction(
                    function.getName(),
                    function.getResolvedFunction(),
                    gen,
                    serializerProvider,
                    serializeCatalogObjects);
        } else if (operator instanceof BridgingSqlAggFunction) {
            final BridgingSqlAggFunction function = (BridgingSqlAggFunction) operator;
            serializeBridgingSqlFunction(
                    function.getName(),
                    function.getResolvedFunction(),
                    gen,
                    serializerProvider,
                    serializeCatalogObjects);
        } else if (operator instanceof ScalarSqlFunction
                || operator instanceof TableSqlFunction
                || operator instanceof AggSqlFunction) {
            throw legacyException(operator.toString());
        } else {
            if (operator.getName().isEmpty()) {
                gen.writeStringField(FIELD_NAME_SQL_KIND, operator.getKind().name());
            } else {
                // We assume that all regular SqlOperators are internal. Only the function
                // definitions
                // stack is exposed to the user and can thus be external.
                gen.writeStringField(
                        FIELD_NAME_INTERNAL_NAME, BuiltInSqlOperator.toQualifiedName(operator));
            }
        }
    }

    private static void serializeBridgingSqlFunction(
            String summaryName,
            ContextResolvedFunction resolvedFunction,
            JsonGenerator gen,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        final FunctionDefinition definition = resolvedFunction.getDefinition();
        if (definition instanceof ScalarFunctionDefinition
                || definition instanceof TableFunctionDefinition
                || definition instanceof TableAggregateFunctionDefinition
                || definition instanceof AggregateFunctionDefinition) {
            throw legacyException(summaryName);
        }

        if (definition instanceof BuiltInFunctionDefinition) {
            final BuiltInFunctionDefinition builtInFunction =
                    (BuiltInFunctionDefinition) definition;
            gen.writeStringField(FIELD_NAME_INTERNAL_NAME, builtInFunction.getQualifiedName());
        } else if (resolvedFunction.isAnonymous()) {
            serializeInlineFunction(summaryName, definition, gen);
        } else if (resolvedFunction.isTemporary()) {
            serializeTemporaryFunction(resolvedFunction, gen, serializerProvider);
        } else {
            assert resolvedFunction.isPermanent();
            serializePermanentFunction(
                    resolvedFunction, gen, serializerProvider, serializeCatalogObjects);
        }
    }

    private static void serializeInlineFunction(
            String summaryName, FunctionDefinition definition, JsonGenerator gen)
            throws IOException {
        if (!(definition instanceof UserDefinedFunction)
                || !isClassNameSerializable((UserDefinedFunction) definition)) {
            throw cannotSerializeInlineFunction(summaryName);
        }
        gen.writeStringField(FIELD_NAME_CLASS, definition.getClass().getName());
    }

    private static void serializeTemporaryFunction(
            ContextResolvedFunction resolvedFunction,
            JsonGenerator gen,
            SerializerProvider serializerProvider)
            throws IOException {
        assert resolvedFunction.getIdentifier().isPresent();
        final FunctionIdentifier identifier = resolvedFunction.getIdentifier().get();
        if (identifier.getSimpleName().isPresent()) {
            gen.writeStringField(FIELD_NAME_SYSTEM_NAME, identifier.getSimpleName().get());
        } else {
            assert identifier.getIdentifier().isPresent();
            serializerProvider.defaultSerializeField(
                    FIELD_NAME_CATALOG_NAME, identifier.getIdentifier().get(), gen);
        }
    }

    private static void serializePermanentFunction(
            ContextResolvedFunction resolvedFunction,
            JsonGenerator gen,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        final FunctionIdentifier identifier =
                resolvedFunction
                        .getIdentifier()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Permanent functions should own a function identifier."));

        if (identifier.getSimpleName().isPresent()) {
            // Module provided system function
            gen.writeStringField(FIELD_NAME_SYSTEM_NAME, identifier.getSimpleName().get());
        } else {
            assert identifier.getIdentifier().isPresent();
            serializeCatalogFunction(
                    identifier.getIdentifier().get(),
                    resolvedFunction.getDefinition(),
                    gen,
                    serializerProvider,
                    serializeCatalogObjects);
        }
    }

    private static void serializeCatalogFunction(
            ObjectIdentifier objectIdentifier,
            FunctionDefinition definition,
            JsonGenerator gen,
            SerializerProvider serializerProvider,
            boolean serializeCatalogObjects)
            throws IOException {
        serializerProvider.defaultSerializeField(FIELD_NAME_CATALOG_NAME, objectIdentifier, gen);

        if (!serializeCatalogObjects) {
            return;
        }

        if (!(definition instanceof UserDefinedFunction)
                || !isClassNameSerializable((UserDefinedFunction) definition)) {
            throw cannotSerializePermanentCatalogFunction(objectIdentifier);
        }

        gen.writeStringField(FIELD_NAME_CLASS, definition.getClass().getName());
    }

    private static TableException legacyException(String summaryName) {
        return new TableException(
                String.format(
                        "Functions of the deprecated function stack are not supported. "
                                + "Please update '%s' to the new interfaces.",
                        summaryName));
    }

    private static TableException cannotSerializeInlineFunction(String summaryName) {
        return new TableException(
                String.format(
                        "Anonymous function '%s' is not serializable. The function's implementation "
                                + "class must not be stateful (i.e. containing only transient and "
                                + "static fields) and should provide a default constructor. One can "
                                + "register the function under a temporary name to resolve this issue.",
                        summaryName));
    }

    private static TableException cannotSerializePermanentCatalogFunction(
            ObjectIdentifier objectIdentifier) {
        return new TableException(
                String.format(
                        "Permanent catalog function '%s' is not serializable. The function's implementation "
                                + "class must not be stateful (i.e. containing only transient and static "
                                + "fields) and should provide a default constructor. Depending on the "
                                + "catalog implementation, it might be necessary to only serialize the "
                                + "function's identifier by setting the option '%s'='%s'. One then needs "
                                + "to guarantee that the same catalog object is also present during a "
                                + "restore.",
                        objectIdentifier.asSummaryString(),
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS.key(),
                        CatalogPlanCompilation.IDENTIFIER));
    }
}
