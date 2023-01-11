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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.sql.FlinkTimestampWithPrecisionDynamicFunction;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

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
    private static Map<Boolean, DirectConvertRule> cachedInstances = new HashMap<>();

    public static synchronized DirectConvertRule instance(boolean isBatchMode) {
        DirectConvertRule instance = cachedInstances.get(isBatchMode);
        if (instance == null) {
            instance = new DirectConvertRule();
            instance.initNonDynamicFunctions();
            instance.initDynamicFunctions(isBatchMode);
            cachedInstances.put(isBatchMode, instance);
        }
        return instance;
    }

    private final Map<FunctionDefinition, SqlOperator> definitionSqlOperatorHashMap =
            new HashMap<>();

    void initDynamicFunctions(boolean isBatchMode) {
        FlinkSqlOperatorTable.dynamicFunctions(isBatchMode)
                .forEach(
                        func -> {
                            if (func.getName()
                                    .equalsIgnoreCase(SqlStdOperatorTable.CURRENT_DATE.getName())) {
                                definitionSqlOperatorHashMap.put(
                                        BuiltInFunctionDefinitions.CURRENT_DATE, func);
                            } else if (func.getName()
                                    .equalsIgnoreCase(SqlStdOperatorTable.CURRENT_TIME.getName())) {
                                definitionSqlOperatorHashMap.put(
                                        BuiltInFunctionDefinitions.CURRENT_TIME, func);
                            } else if (func.getName()
                                    .equalsIgnoreCase(SqlStdOperatorTable.LOCALTIME.getName())) {
                                definitionSqlOperatorHashMap.put(
                                        BuiltInFunctionDefinitions.LOCAL_TIME, func);
                            } else if (func.getName()
                                    .equalsIgnoreCase(
                                            SqlStdOperatorTable.CURRENT_TIMESTAMP.getName())) {
                                definitionSqlOperatorHashMap.put(
                                        BuiltInFunctionDefinitions.CURRENT_TIMESTAMP, func);
                            } else if (func.getName()
                                    .equalsIgnoreCase(
                                            SqlStdOperatorTable.LOCALTIMESTAMP.getName())) {
                                definitionSqlOperatorHashMap.put(
                                        BuiltInFunctionDefinitions.LOCAL_TIMESTAMP, func);
                            } else if (func.getName()
                                    .equalsIgnoreCase(
                                            FlinkTimestampWithPrecisionDynamicFunction.NOW)) {
                                definitionSqlOperatorHashMap.put(
                                        BuiltInFunctionDefinitions.NOW, func);
                            } else {
                                throw new TableException(
                                        String.format(
                                                "Unsupported mapping for dynamic function: %s",
                                                func.getName()));
                            }
                        });
    }

    void initNonDynamicFunctions() {
        // logic functions
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.AND, FlinkSqlOperatorTable.AND);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.OR, FlinkSqlOperatorTable.OR);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.NOT, FlinkSqlOperatorTable.NOT);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.IF, FlinkSqlOperatorTable.CASE);

        // comparison functions
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.EQUALS, FlinkSqlOperatorTable.EQUALS);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.GREATER_THAN, FlinkSqlOperatorTable.GREATER_THAN);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LESS_THAN, FlinkSqlOperatorTable.LESS_THAN);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.NOT_EQUALS, FlinkSqlOperatorTable.NOT_EQUALS);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.IS_NULL, FlinkSqlOperatorTable.IS_NULL);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.IS_NOT_NULL, FlinkSqlOperatorTable.IS_NOT_NULL);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.IS_TRUE, FlinkSqlOperatorTable.IS_TRUE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.IS_FALSE, FlinkSqlOperatorTable.IS_FALSE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.IS_NOT_TRUE, FlinkSqlOperatorTable.IS_NOT_TRUE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.IS_NOT_FALSE, FlinkSqlOperatorTable.IS_NOT_FALSE);

        // string functions
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.CHAR_LENGTH, FlinkSqlOperatorTable.CHAR_LENGTH);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.INIT_CAP, FlinkSqlOperatorTable.INITCAP);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LIKE, FlinkSqlOperatorTable.LIKE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LOWER, FlinkSqlOperatorTable.LOWER);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LOWERCASE, FlinkSqlOperatorTable.LOWER);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SIMILAR, FlinkSqlOperatorTable.SIMILAR_TO);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SUBSTRING, FlinkSqlOperatorTable.SUBSTRING);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SUBSTR, FlinkSqlOperatorTable.SUBSTR);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.UPPER, FlinkSqlOperatorTable.UPPER);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.UPPERCASE, FlinkSqlOperatorTable.UPPER);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.POSITION, FlinkSqlOperatorTable.POSITION);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.OVERLAY, FlinkSqlOperatorTable.OVERLAY);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.CONCAT, FlinkSqlOperatorTable.CONCAT_FUNCTION);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.CONCAT_WS, FlinkSqlOperatorTable.CONCAT_WS);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LPAD, FlinkSqlOperatorTable.LPAD);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.RPAD, FlinkSqlOperatorTable.RPAD);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.REGEXP_EXTRACT, FlinkSqlOperatorTable.REGEXP_EXTRACT);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.FROM_BASE64, FlinkSqlOperatorTable.FROM_BASE64);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.TO_BASE64, FlinkSqlOperatorTable.TO_BASE64);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ASCII, FlinkSqlOperatorTable.ASCII);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.CHR, FlinkSqlOperatorTable.CHR);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.DECODE, FlinkSqlOperatorTable.DECODE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ENCODE, FlinkSqlOperatorTable.ENCODE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LEFT, FlinkSqlOperatorTable.LEFT);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.RIGHT, FlinkSqlOperatorTable.RIGHT);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.INSTR, FlinkSqlOperatorTable.INSTR);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LOCATE, FlinkSqlOperatorTable.LOCATE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.PARSE_URL, FlinkSqlOperatorTable.PARSE_URL);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.UUID, FlinkSqlOperatorTable.UUID);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LTRIM, FlinkSqlOperatorTable.LTRIM);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.RTRIM, FlinkSqlOperatorTable.RTRIM);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.REPEAT, FlinkSqlOperatorTable.REPEAT);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.REGEXP, FlinkSqlOperatorTable.REGEXP);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.REGEXP_REPLACE, FlinkSqlOperatorTable.REGEXP_REPLACE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.REVERSE, FlinkSqlOperatorTable.REVERSE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SPLIT_INDEX, FlinkSqlOperatorTable.SPLIT_INDEX);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.STR_TO_MAP, FlinkSqlOperatorTable.STR_TO_MAP);

        // math functions
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.MINUS, FlinkSqlOperatorTable.MINUS);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.DIVIDE, FlinkSqlOperatorTable.DIVIDE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.TIMES, FlinkSqlOperatorTable.MULTIPLY);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.FLOOR, FlinkSqlOperatorTable.FLOOR);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.CEIL, FlinkSqlOperatorTable.CEIL);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.ABS, FlinkSqlOperatorTable.ABS);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.EXP, FlinkSqlOperatorTable.EXP);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LOG10, FlinkSqlOperatorTable.LOG10);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.LOG2, FlinkSqlOperatorTable.LOG2);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.LN, FlinkSqlOperatorTable.LN);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.LOG, FlinkSqlOperatorTable.LOG);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.POWER, FlinkSqlOperatorTable.POWER);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.MOD, FlinkSqlOperatorTable.MOD);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.MINUS_PREFIX, FlinkSqlOperatorTable.UNARY_MINUS);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.SIN, FlinkSqlOperatorTable.SIN);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.COS, FlinkSqlOperatorTable.COS);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SINH, FlinkSqlOperatorTable.SINH);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.TAN, FlinkSqlOperatorTable.TAN);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.TANH, FlinkSqlOperatorTable.TANH);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.COT, FlinkSqlOperatorTable.COT);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ASIN, FlinkSqlOperatorTable.ASIN);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ACOS, FlinkSqlOperatorTable.ACOS);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ATAN, FlinkSqlOperatorTable.ATAN);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ATAN2, FlinkSqlOperatorTable.ATAN2);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.COSH, FlinkSqlOperatorTable.COSH);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.DEGREES, FlinkSqlOperatorTable.DEGREES);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.RADIANS, FlinkSqlOperatorTable.RADIANS);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SIGN, FlinkSqlOperatorTable.SIGN);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ROUND, FlinkSqlOperatorTable.ROUND);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.PI, FlinkSqlOperatorTable.PI);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.E, FlinkSqlOperatorTable.E);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.RAND, FlinkSqlOperatorTable.RAND);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.RAND_INTEGER, FlinkSqlOperatorTable.RAND_INTEGER);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.BIN, FlinkSqlOperatorTable.BIN);
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.HEX, FlinkSqlOperatorTable.HEX);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.TRUNCATE, FlinkSqlOperatorTable.TRUNCATE);

        // time functions
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.EXTRACT, FlinkSqlOperatorTable.EXTRACT);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.CURRENT_ROW_TIMESTAMP,
                FlinkSqlOperatorTable.CURRENT_ROW_TIMESTAMP);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.DATE_FORMAT, FlinkSqlOperatorTable.DATE_FORMAT);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.CONVERT_TZ, FlinkSqlOperatorTable.CONVERT_TZ);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.FROM_UNIXTIME, FlinkSqlOperatorTable.FROM_UNIXTIME);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.UNIX_TIMESTAMP, FlinkSqlOperatorTable.UNIX_TIMESTAMP);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.TO_DATE, FlinkSqlOperatorTable.TO_DATE);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.TO_TIMESTAMP_LTZ,
                FlinkSqlOperatorTable.TO_TIMESTAMP_LTZ);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.TO_TIMESTAMP, FlinkSqlOperatorTable.TO_TIMESTAMP);

        // catalog functions
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.CURRENT_DATABASE,
                FlinkSqlOperatorTable.CURRENT_DATABASE);

        // collection
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.AT, FlinkSqlOperatorTable.ITEM);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.CARDINALITY, FlinkSqlOperatorTable.CARDINALITY);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ORDER_DESC, FlinkSqlOperatorTable.DESC);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.ARRAY_ELEMENT, FlinkSqlOperatorTable.ELEMENT);

        // crypto hash
        definitionSqlOperatorHashMap.put(BuiltInFunctionDefinitions.MD5, FlinkSqlOperatorTable.MD5);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SHA2, FlinkSqlOperatorTable.SHA2);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SHA224, FlinkSqlOperatorTable.SHA224);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SHA256, FlinkSqlOperatorTable.SHA256);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SHA384, FlinkSqlOperatorTable.SHA384);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SHA512, FlinkSqlOperatorTable.SHA512);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.SHA1, FlinkSqlOperatorTable.SHA1);
        definitionSqlOperatorHashMap.put(
                BuiltInFunctionDefinitions.STREAM_RECORD_TIMESTAMP,
                FlinkSqlOperatorTable.STREAMRECORD_TIMESTAMP);
    }

    @Override
    public Optional<RexNode> convert(CallExpression call, ConvertContext context) {
        SqlOperator operator = definitionSqlOperatorHashMap.get(call.getFunctionDefinition());
        return Optional.ofNullable(operator)
                .map(
                        op ->
                                context.getRelBuilder()
                                        .call(op, toRexNodes(context, call.getChildren())));
    }
}
