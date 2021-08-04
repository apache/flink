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

import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;

import java.io.IOException;

/**
 * JSON serializer for {@link AggregateCall}. refer to {@link AggregateCallJsonDeserializer} for
 * deserializer.
 */
public class AggregateCallJsonSerializer extends StdSerializer<AggregateCall> {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_KIND = "kind";
    public static final String FIELD_NAME_TYPE = "type";
    public static final String FIELD_NAME_NAME = "name";

    public static final String FIELD_NAME_AGG_FUNCTION = "aggFunction";
    public static final String FIELD_NAME_INSTANCE = "instance";
    public static final String FIELD_NAME_SYNTAX = "syntax";
    public static final String FIELD_NAME_DISPLAY_NAME = "displayName";
    public static final String FIELD_NAME_FUNCTION_KIND = "functionKind";
    public static final String FIELD_NAME_BRIDGING = "bridging";
    public static final String FIELD_NAME_BUILT_IN = "builtIn";
    public static final String FIELD_NAME_REQUIRES_OVER = "requiresOver";

    public static final String FIELD_NAME_ARG_LIST = "argList";
    public static final String FIELD_NAME_FILTER_ARG = "filterArg";
    public static final String FIELD_NAME_DISTINCT = "distinct";
    public static final String FIELD_NAME_APPROXIMATE = "approximate";
    public static final String FIELD_NAME_IGNORE_NULLS = "ignoreNulls";

    public AggregateCallJsonSerializer() {
        super(AggregateCall.class);
    }

    @Override
    public void serialize(
            AggregateCall aggCall,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField(FIELD_NAME_NAME, aggCall.getName());
        serialize(aggCall.getAggregation(), jsonGenerator);
        jsonGenerator.writeFieldName(FIELD_NAME_ARG_LIST);
        jsonGenerator.writeStartArray();
        for (int arg : aggCall.getArgList()) {
            jsonGenerator.writeNumber(arg);
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeNumberField(FIELD_NAME_FILTER_ARG, aggCall.filterArg);
        jsonGenerator.writeBooleanField(FIELD_NAME_DISTINCT, aggCall.isDistinct());
        jsonGenerator.writeBooleanField(FIELD_NAME_APPROXIMATE, aggCall.isApproximate());
        jsonGenerator.writeBooleanField(FIELD_NAME_IGNORE_NULLS, aggCall.ignoreNulls());
        jsonGenerator.writeObjectField(FIELD_NAME_TYPE, aggCall.getType());
        jsonGenerator.writeEndObject();
    }

    private void serialize(SqlAggFunction operator, JsonGenerator gen) throws IOException {
        gen.writeFieldName(FIELD_NAME_AGG_FUNCTION);
        gen.writeStartObject();
        gen.writeStringField(FIELD_NAME_NAME, operator.getName());
        gen.writeStringField(FIELD_NAME_KIND, operator.kind.name());
        gen.writeStringField(FIELD_NAME_SYNTAX, operator.getSyntax().name());
        // TODO if a udf is registered with class name, class name is recorded enough
        if (operator instanceof AggSqlFunction) {
            AggSqlFunction aggSqlFunc = (AggSqlFunction) operator;
            gen.writeStringField(FIELD_NAME_DISPLAY_NAME, aggSqlFunc.displayName());
            gen.writeStringField(FIELD_NAME_FUNCTION_KIND, FunctionKind.AGGREGATE.name());
            gen.writeBooleanField(FIELD_NAME_REQUIRES_OVER, aggSqlFunc.requiresOver());
            gen.writeStringField(
                    FIELD_NAME_INSTANCE,
                    EncodingUtils.encodeObjectToString(aggSqlFunc.aggregateFunction()));
        } else if (operator instanceof BridgingSqlAggFunction) {
            BridgingSqlAggFunction bridgingSqlAggFunc = (BridgingSqlAggFunction) operator;
            FunctionDefinition functionDefinition = bridgingSqlAggFunc.getDefinition();
            if (functionDefinition instanceof BuiltInFunctionDefinition) {
                // just record the flag, we can find it by name
                gen.writeBooleanField(FIELD_NAME_BUILT_IN, true);
            } else {
                assert functionDefinition.getKind() == FunctionKind.AGGREGATE;
                gen.writeStringField(FIELD_NAME_FUNCTION_KIND, FunctionKind.AGGREGATE.name());
                gen.writeStringField(
                        FIELD_NAME_INSTANCE,
                        EncodingUtils.encodeObjectToString(functionDefinition));
                gen.writeBooleanField(FIELD_NAME_BRIDGING, true);
            }
        }
        gen.writeEndObject();
    }
}
