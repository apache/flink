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

package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.toRexNodes;

/**
 * A {@link CallExpressionConvertRule} that performs a simple one-to-one mapping between {@link
 * FunctionDefinition} and a corresponding {@link SqlOperator}.
 */
@Internal
public class DirectConvertRule implements CallExpressionConvertRule {

    private static final Map<FunctionDefinition, SqlOperator> DEFINITION_OPERATOR_MAP =
            new HashMap<>();

    static {
        // logic functions
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.AND, FlinkSqlOperatorTable.AND);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.OR, FlinkSqlOperatorTable.OR);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.NOT, FlinkSqlOperatorTable.NOT);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.IF, FlinkSqlOperatorTable.CASE);

        // comparison functions
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.EQUALS, FlinkSqlOperatorTable.EQUALS);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.GREATER_THAN, FlinkSqlOperatorTable.GREATER_THAN);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.LESS_THAN, FlinkSqlOperatorTable.LESS_THAN);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.NOT_EQUALS, FlinkSqlOperatorTable.NOT_EQUALS);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.IS_NULL, FlinkSqlOperatorTable.IS_NULL);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.IS_NOT_NULL, FlinkSqlOperatorTable.IS_NOT_NULL);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.IS_TRUE, FlinkSqlOperatorTable.IS_TRUE);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.IS_FALSE, FlinkSqlOperatorTable.IS_FALSE);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.IS_NOT_TRUE, FlinkSqlOperatorTable.IS_NOT_TRUE);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.IS_NOT_FALSE, FlinkSqlOperatorTable.IS_NOT_FALSE);

        // string functions
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.CHAR_LENGTH, FlinkSqlOperatorTable.CHAR_LENGTH);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.INIT_CAP, FlinkSqlOperatorTable.INITCAP);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.LIKE, FlinkSqlOperatorTable.LIKE);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.LOWER, FlinkSqlOperatorTable.LOWER);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.LOWERCASE, FlinkSqlOperatorTable.LOWER);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.SIMILAR, FlinkSqlOperatorTable.SIMILAR_TO);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.SUBSTRING, FlinkSqlOperatorTable.SUBSTRING);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.UPPER, FlinkSqlOperatorTable.UPPER);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.UPPERCASE, FlinkSqlOperatorTable.UPPER);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.POSITION, FlinkSqlOperatorTable.POSITION);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.OVERLAY, FlinkSqlOperatorTable.OVERLAY);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.CONCAT, FlinkSqlOperatorTable.CONCAT_FUNCTION);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.CONCAT_WS, FlinkSqlOperatorTable.CONCAT_WS);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.LPAD, FlinkSqlOperatorTable.LPAD);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.RPAD, FlinkSqlOperatorTable.RPAD);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.REGEXP_EXTRACT, FlinkSqlOperatorTable.REGEXP_EXTRACT);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.FROM_BASE64, FlinkSqlOperatorTable.FROM_BASE64);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.TO_BASE64, FlinkSqlOperatorTable.TO_BASE64);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.UUID, FlinkSqlOperatorTable.UUID);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.LTRIM, FlinkSqlOperatorTable.LTRIM);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.RTRIM, FlinkSqlOperatorTable.RTRIM);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.REPEAT, FlinkSqlOperatorTable.REPEAT);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.REGEXP_REPLACE, FlinkSqlOperatorTable.REGEXP_REPLACE);

        // math functions
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.MINUS, FlinkSqlOperatorTable.MINUS);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.DIVIDE, FlinkSqlOperatorTable.DIVIDE);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.TIMES, FlinkSqlOperatorTable.MULTIPLY);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.FLOOR, FlinkSqlOperatorTable.FLOOR);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.CEIL, FlinkSqlOperatorTable.CEIL);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.ABS, FlinkSqlOperatorTable.ABS);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.EXP, FlinkSqlOperatorTable.EXP);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.LOG10, FlinkSqlOperatorTable.LOG10);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.LOG2, FlinkSqlOperatorTable.LOG2);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.LN, FlinkSqlOperatorTable.LN);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.LOG, FlinkSqlOperatorTable.LOG);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.POWER, FlinkSqlOperatorTable.POWER);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.MOD, FlinkSqlOperatorTable.MOD);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.MINUS_PREFIX, FlinkSqlOperatorTable.UNARY_MINUS);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.SIN, FlinkSqlOperatorTable.SIN);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.COS, FlinkSqlOperatorTable.COS);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.SINH, FlinkSqlOperatorTable.SINH);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.TAN, FlinkSqlOperatorTable.TAN);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.TANH, FlinkSqlOperatorTable.TANH);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.COT, FlinkSqlOperatorTable.COT);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.ASIN, FlinkSqlOperatorTable.ASIN);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.ACOS, FlinkSqlOperatorTable.ACOS);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.ATAN, FlinkSqlOperatorTable.ATAN);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.ATAN2, FlinkSqlOperatorTable.ATAN2);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.COSH, FlinkSqlOperatorTable.COSH);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.DEGREES, FlinkSqlOperatorTable.DEGREES);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.RADIANS, FlinkSqlOperatorTable.RADIANS);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.SIGN, FlinkSqlOperatorTable.SIGN);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.ROUND, FlinkSqlOperatorTable.ROUND);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.PI, FlinkSqlOperatorTable.PI);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.E, FlinkSqlOperatorTable.E);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.RAND, FlinkSqlOperatorTable.RAND);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.RAND_INTEGER, FlinkSqlOperatorTable.RAND_INTEGER);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.BIN, FlinkSqlOperatorTable.BIN);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.HEX, FlinkSqlOperatorTable.HEX);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.TRUNCATE, FlinkSqlOperatorTable.TRUNCATE);

        // time functions
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.EXTRACT, FlinkSqlOperatorTable.EXTRACT);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.CURRENT_DATE, FlinkSqlOperatorTable.CURRENT_DATE);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.CURRENT_TIME, FlinkSqlOperatorTable.CURRENT_TIME);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.CURRENT_TIMESTAMP,
                FlinkSqlOperatorTable.CURRENT_TIMESTAMP);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.CURRENT_ROW_TIMESTAMP,
                FlinkSqlOperatorTable.CURRENT_ROW_TIMESTAMP);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.LOCAL_TIME, FlinkSqlOperatorTable.LOCALTIME);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.LOCAL_TIMESTAMP, FlinkSqlOperatorTable.LOCALTIMESTAMP);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.DATE_FORMAT, FlinkSqlOperatorTable.DATE_FORMAT);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.TO_TIMESTAMP_LTZ,
                FlinkSqlOperatorTable.TO_TIMESTAMP_LTZ);

        // collection
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.AT, FlinkSqlOperatorTable.ITEM);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.CARDINALITY, FlinkSqlOperatorTable.CARDINALITY);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.ORDER_DESC, FlinkSqlOperatorTable.DESC);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.ARRAY_ELEMENT, FlinkSqlOperatorTable.ELEMENT);

        // crypto hash
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.MD5, FlinkSqlOperatorTable.MD5);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.SHA2, FlinkSqlOperatorTable.SHA2);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.SHA224, FlinkSqlOperatorTable.SHA224);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.SHA256, FlinkSqlOperatorTable.SHA256);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.SHA384, FlinkSqlOperatorTable.SHA384);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.SHA512, FlinkSqlOperatorTable.SHA512);
        DEFINITION_OPERATOR_MAP.put(BuiltInFunctionDefinitions.SHA1, FlinkSqlOperatorTable.SHA1);
        DEFINITION_OPERATOR_MAP.put(
                BuiltInFunctionDefinitions.STREAM_RECORD_TIMESTAMP,
                FlinkSqlOperatorTable.STREAMRECORD_TIMESTAMP);
    }

    @Override
    public Optional<RexNode> convert(CallExpression call, ConvertContext context) {
        SqlOperator operator = DEFINITION_OPERATOR_MAP.get(call.getFunctionDefinition());
        return Optional.ofNullable(operator)
                .map(
                        op ->
                                context.getRelBuilder()
                                        .call(op, toRexNodes(context, call.getChildren())));
    }
}
