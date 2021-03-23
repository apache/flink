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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * JSON serializer for {@link RexNode}. refer to {@link RexNodeJsonDeserializer} for deserializer.
 */
public class RexNodeJsonSerializer extends StdSerializer<RexNode> {
    private static final long serialVersionUID = 1L;

    // common fields
    public static final String FIELD_NAME_KIND = "kind";
    public static final String FIELD_NAME_VALUE = "value";
    public static final String FIELD_NAME_TYPE = "type";
    public static final String FIELD_NAME_NAME = "name";
    // RexInputRef fields
    public static final String FIELD_NAME_INPUT_INDEX = "inputIndex";
    // RexPatternFieldRef fields
    public static final String FIELD_NAME_ALPHA = "alpha";
    // RexLiteral fields
    public static final String FIELD_NAME_CLASS = "class";
    // RexFieldAccess fields
    public static final String FIELD_NAME_EXPR = "expr";
    // RexCorrelVariable fields
    public static final String FIELD_NAME_CORREL = "correl";
    // RexCall fields
    public static final String FIELD_NAME_OPERATOR = "operator";
    public static final String FIELD_NAME_OPERANDS = "operands";
    public static final String FIELD_NAME_SYNTAX = "syntax";
    public static final String FIELD_NAME_DISPLAY_NAME = "displayName";
    public static final String FIELD_NAME_FUNCTION_KIND = "functionKind";
    public static final String FIELD_NAME_INSTANCE = "instance";
    public static final String FIELD_NAME_BRIDGING = "bridging";
    public static final String FIELD_NAME_BUILT_IN = "builtIn";

    // Sarg fields and values
    public static final String FIELD_NAME_SARG = "sarg";
    public static final String FIELD_NAME_RANGES = "ranges";
    public static final String FIELD_NAME_BOUND_LOWER = "lower";
    public static final String FIELD_NAME_BOUND_UPPER = "upper";
    public static final String FIELD_NAME_BOUND_TYPE = "boundType";
    public static final String FIELD_NAME_CONTAINS_NULL = "containsNull";

    // supported SqlKinds
    public static final String SQL_KIND_PATTERN_INPUT_REF = "PATTERN_INPUT_REF";
    public static final String SQL_KIND_INPUT_REF = "INPUT_REF";
    public static final String SQL_KIND_LITERAL = "LITERAL";
    public static final String SQL_KIND_FIELD_ACCESS = "FIELD_ACCESS";
    public static final String SQL_KIND_CORREL_VARIABLE = "CORREL_VARIABLE";
    public static final String SQL_KIND_REX_CALL = "REX_CALL";

    public RexNodeJsonSerializer() {
        super(RexNode.class);
    }

    @Override
    public void serialize(
            RexNode rexNode, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {

        switch (rexNode.getKind()) {
            case INPUT_REF:
            case TABLE_INPUT_REF:
                serialize((RexInputRef) rexNode, jsonGenerator);
                break;
            case LITERAL:
                serialize((RexLiteral) rexNode, jsonGenerator);
                break;
            case FIELD_ACCESS:
                serialize((RexFieldAccess) rexNode, jsonGenerator);
                break;
            case CORREL_VARIABLE:
                serialize((RexCorrelVariable) rexNode, jsonGenerator);
                break;
            case PATTERN_INPUT_REF:
                serialize((RexPatternFieldRef) rexNode, jsonGenerator);
                break;
            default:
                if (rexNode instanceof RexCall) {
                    serialize((RexCall) rexNode, jsonGenerator);
                } else {
                    throw new TableException("Unknown RexNode: " + rexNode);
                }
        }
    }

    private void serialize(RexPatternFieldRef inputRef, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, SQL_KIND_PATTERN_INPUT_REF);
        gen.writeStringField(FIELD_NAME_ALPHA, inputRef.getAlpha());
        gen.writeNumberField(FIELD_NAME_INPUT_INDEX, inputRef.getIndex());
        gen.writeObjectField(FIELD_NAME_TYPE, inputRef.getType());
        gen.writeEndObject();
    }

    private void serialize(RexInputRef inputRef, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, SQL_KIND_INPUT_REF);
        gen.writeNumberField(FIELD_NAME_INPUT_INDEX, inputRef.getIndex());
        gen.writeObjectField(FIELD_NAME_TYPE, inputRef.getType());
        gen.writeEndObject();
    }

    private void serialize(RexLiteral literal, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, SQL_KIND_LITERAL);
        Comparable<?> value = literal.getValueAs(Comparable.class);
        serialize(value, literal.getTypeName(), literal.getType().getSqlTypeName(), gen);
        gen.writeObjectField(FIELD_NAME_TYPE, literal.getType());
        gen.writeEndObject();
    }

    @SuppressWarnings("unchecked")
    private void serialize(
            Comparable<?> value,
            SqlTypeName literalTypeName,
            SqlTypeName elementTypeName,
            JsonGenerator gen)
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
                gen.writeNumberField(FIELD_NAME_VALUE, ((BigDecimal) value).doubleValue());
                break;
            case FLOAT:
                gen.writeNumberField(FIELD_NAME_VALUE, ((BigDecimal) value).floatValue());
                break;
            case DECIMAL:
            case REAL:
                // use String to make sure that no data is lost
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
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // store string value to avoid data lost
                gen.writeStringField(FIELD_NAME_VALUE, value.toString());
                break;
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
                gen.writeNumberField(FIELD_NAME_VALUE, ((BigDecimal) value).longValue());
                break;
            case SYMBOL:
                gen.writeStringField(FIELD_NAME_VALUE, ((Enum<?>) value).name());
                gen.writeStringField(FIELD_NAME_CLASS, value.getClass().getName());
                break;
            case SARG:
                serialize((Sarg<?>) value, elementTypeName, gen);
                break;
            case ROW:
            case MULTISET:
                gen.writeFieldName(FIELD_NAME_VALUE);
                gen.writeStartArray();
                for (RexLiteral v : (FlatLists.ComparableList<RexLiteral>) value) {
                    serialize(v, gen);
                }
                gen.writeEndArray();
                break;
            default:
                // TODO support ARRAY, MAP
                throw new TableException("Unknown value: " + value + ", type: " + literalTypeName);
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private void serialize(Sarg<?> value, SqlTypeName sqlTypeName, JsonGenerator gen)
            throws IOException {
        gen.writeFieldName(FIELD_NAME_SARG);
        gen.writeStartObject();
        gen.writeFieldName(FIELD_NAME_RANGES);
        gen.writeStartArray();
        for (com.google.common.collect.Range<?> range : value.rangeSet.asRanges()) {
            gen.writeStartObject();
            if (range.hasLowerBound()) {
                gen.writeFieldName(FIELD_NAME_BOUND_LOWER);
                gen.writeStartObject();
                serialize(range.lowerEndpoint(), sqlTypeName, sqlTypeName, gen);
                gen.writeStringField(FIELD_NAME_BOUND_TYPE, range.lowerBoundType().name());
                gen.writeEndObject();
            }
            if (range.hasUpperBound()) {
                gen.writeFieldName(FIELD_NAME_BOUND_UPPER);
                gen.writeStartObject();
                serialize(range.upperEndpoint(), sqlTypeName, sqlTypeName, gen);
                gen.writeStringField(FIELD_NAME_BOUND_TYPE, range.upperBoundType().name());
                gen.writeEndObject();
            }
            gen.writeEndObject();
        }
        gen.writeEndArray();
        gen.writeBooleanField(FIELD_NAME_CONTAINS_NULL, value.containsNull);
        gen.writeEndObject();
    }

    private void serialize(RexFieldAccess fieldAccess, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, SQL_KIND_FIELD_ACCESS);
        gen.writeStringField(FIELD_NAME_NAME, fieldAccess.getField().getName());
        gen.writeObjectField(FIELD_NAME_EXPR, fieldAccess.getReferenceExpr());
        gen.writeEndObject();
    }

    private void serialize(RexCorrelVariable variable, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, SQL_KIND_CORREL_VARIABLE);
        gen.writeStringField(FIELD_NAME_CORREL, variable.getName());
        gen.writeObjectField(FIELD_NAME_TYPE, variable.getType());
        gen.writeEndObject();
    }

    private void serialize(RexCall call, JsonGenerator gen) throws IOException {
        if (!call.getClass().isAssignableFrom(RexCall.class)) {
            throw new TableException("Unknown RexCall: " + call);
        }
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_KIND, SQL_KIND_REX_CALL);
        serialize(call.getOperator(), gen);
        gen.writeFieldName(FIELD_NAME_OPERANDS);
        gen.writeStartArray();
        for (RexNode operand : call.getOperands()) {
            gen.writeObject(operand);
        }
        gen.writeEndArray();
        gen.writeObjectField(FIELD_NAME_TYPE, call.getType());
        gen.writeEndObject();
    }

    private void serialize(SqlOperator operator, JsonGenerator gen) throws IOException {
        gen.writeFieldName(FIELD_NAME_OPERATOR);
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_NAME, operator.getName());
        gen.writeStringField(FIELD_NAME_KIND, operator.kind.name());
        gen.writeStringField(FIELD_NAME_SYNTAX, operator.getSyntax().name());
        // TODO if a udf is registered with class name, class name is recorded enough
        if (operator instanceof ScalarSqlFunction) {
            ScalarSqlFunction scalarSqlFunc = (ScalarSqlFunction) operator;
            gen.writeStringField(FIELD_NAME_DISPLAY_NAME, scalarSqlFunc.displayName());
            gen.writeStringField(FIELD_NAME_FUNCTION_KIND, FunctionKind.SCALAR.name());
            gen.writeStringField(
                    FIELD_NAME_INSTANCE,
                    EncodingUtils.encodeObjectToString(scalarSqlFunc.scalarFunction()));
        } else if (operator instanceof BridgingSqlFunction) {
            BridgingSqlFunction bridgingSqlFunc = (BridgingSqlFunction) operator;
            FunctionDefinition functionDefinition = bridgingSqlFunc.getDefinition();
            if (functionDefinition instanceof BuiltInFunctionDefinition) {
                // just record the flag, we can find it by name
                gen.writeBooleanField(FIELD_NAME_BUILT_IN, true);
            } else {
                gen.writeStringField(FIELD_NAME_FUNCTION_KIND, functionDefinition.getKind().name());
                gen.writeStringField(
                        FIELD_NAME_INSTANCE,
                        EncodingUtils.encodeObjectToString(functionDefinition));
                gen.writeBooleanField(FIELD_NAME_BRIDGING, true);
            }
        }
        gen.writeEndObject();
    }
}
