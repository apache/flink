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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ImperativeAggregateFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_AGG_FUNCTION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_APPROXIMATE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_ARG_LIST;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_BRIDGING;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_BUILT_IN;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_DISPLAY_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_DISTINCT;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_FILTER_ARG;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_FUNCTION_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_IGNORE_NULLS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_INSTANCE;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_KIND;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_NAME;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_REQUIRES_OVER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_SYNTAX;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.AggregateCallJsonSerializer.FIELD_NAME_TYPE;

/**
 * JSON deserializer for {@link AggregateCall}. refer to {@link AggregateCallJsonSerializer} for
 * serializer.
 */
public class AggregateCallJsonDeserializer extends StdDeserializer<AggregateCall> {
    private static final long serialVersionUID = 1L;

    public AggregateCallJsonDeserializer() {
        super(AggregateCall.class);
    }

    @Override
    public AggregateCall deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException, JsonProcessingException {
        JsonNode jsonNode = jsonParser.readValueAsTree();
        JsonNode aggFunNode = jsonNode.get(FIELD_NAME_AGG_FUNCTION);
        SqlAggFunction aggFunction =
                toSqlAggFunction(aggFunNode, ((FlinkDeserializationContext) ctx).getSerdeContext());

        List<Integer> argList = new ArrayList<>();
        JsonNode argListNode = jsonNode.get(FIELD_NAME_ARG_LIST);
        for (JsonNode argNode : argListNode) {
            argList.add(argNode.intValue());
        }
        int filterArg = jsonNode.get(FIELD_NAME_FILTER_ARG).intValue();
        boolean distinct = jsonNode.get(FIELD_NAME_DISTINCT).asBoolean();
        boolean approximate = jsonNode.get(FIELD_NAME_APPROXIMATE).asBoolean();
        boolean ignoreNulls = jsonNode.get(FIELD_NAME_IGNORE_NULLS).asBoolean();
        JsonNode typeNode = jsonNode.get(FIELD_NAME_TYPE);
        RelDataType relDataType =
                ((FlinkDeserializationContext) ctx)
                        .getObjectMapper()
                        .readValue(typeNode.toPrettyString(), RelDataType.class);
        String name = jsonNode.get(FIELD_NAME_NAME).asText();

        return AggregateCall.create(
                aggFunction,
                distinct,
                approximate,
                ignoreNulls,
                argList,
                filterArg,
                RelCollations.EMPTY,
                relDataType,
                name);
    }

    private SqlAggFunction toSqlAggFunction(JsonNode jsonNode, SerdeContext ctx)
            throws IOException {
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
                return (SqlAggFunction) operator;
            }
        }

        DataTypeFactory dataTypeFactory =
                ctx.getFlinkContext().getCatalogManager().getDataTypeFactory();

        // built-in function
        // TODO supports other module's built-in function
        if (jsonNode.has(FIELD_NAME_BUILT_IN) && jsonNode.get(FIELD_NAME_BUILT_IN).booleanValue()) {
            Optional<FunctionDefinition> definition =
                    CoreModule.INSTANCE.getFunctionDefinition(name);
            Preconditions.checkArgument(definition.isPresent());
            TypeInference typeInference = definition.get().getTypeInference(dataTypeFactory);
            return BridgingSqlAggFunction.of(
                    dataTypeFactory,
                    ctx.getTypeFactory(),
                    sqlKind,
                    FunctionIdentifier.of(name),
                    definition.get(),
                    typeInference);
        }

        if (jsonNode.has(FIELD_NAME_FUNCTION_KIND) && jsonNode.has(FIELD_NAME_INSTANCE)) {
            FunctionKind functionKind =
                    FunctionKind.valueOf(
                            jsonNode.get(FIELD_NAME_FUNCTION_KIND).asText().toUpperCase());
            String instanceStr = jsonNode.get(FIELD_NAME_INSTANCE).asText();
            if (functionKind != FunctionKind.AGGREGATE) {
                throw new TableException("Unknown function kind: " + functionKind);
            }
            if (jsonNode.has(FIELD_NAME_BRIDGING)
                    && jsonNode.get(FIELD_NAME_BRIDGING).booleanValue()) {
                FunctionDefinition definition =
                        EncodingUtils.decodeStringToObject(instanceStr, ctx.getClassLoader());
                TypeInference typeInference = definition.getTypeInference(dataTypeFactory);
                return BridgingSqlAggFunction.of(
                        dataTypeFactory,
                        ctx.getTypeFactory(),
                        sqlKind,
                        FunctionIdentifier.of(name),
                        definition,
                        typeInference);
            } else {
                String displayName = jsonNode.get(FIELD_NAME_DISPLAY_NAME).asText();
                boolean requiresOver = jsonNode.get(FIELD_NAME_REQUIRES_OVER).booleanValue();
                ImperativeAggregateFunction<?, ?> function =
                        EncodingUtils.decodeStringToObject(instanceStr, ctx.getClassLoader());
                return AggSqlFunction.apply(
                        FunctionIdentifier.of(name),
                        displayName,
                        function,
                        TypeConversions.fromLegacyInfoToDataType(
                                UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(
                                        function)),
                        TypeConversions.fromLegacyInfoToDataType(
                                UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(
                                        function)),
                        ctx.getTypeFactory(),
                        requiresOver);
            }
        }
        throw new TableException("Unknown operator: " + jsonNode.toPrettyString());
    }
}
