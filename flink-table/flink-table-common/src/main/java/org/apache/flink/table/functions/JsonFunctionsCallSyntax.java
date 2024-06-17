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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonType;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.functions.CallSyntaxUtils.getSymbolLiteral;

/** Implementations of {@link SqlCallSyntax} specific for JSON functions. */
@Internal
class JsonFunctionsCallSyntax {

    static final SqlCallSyntax IS_JSON =
            (sqlName, operands) -> {
                final String s =
                        String.format(
                                "%s IS JSON",
                                CallSyntaxUtils.asSerializableOperand(operands.get(0)));
                if (operands.size() > 1) {
                    return s + " " + getSymbolLiteral(operands.get(1), JsonType.class);
                }

                return s;
            };

    static final SqlCallSyntax JSON_VALUE =
            (sqlName, operands) -> {
                StringBuilder s =
                        new StringBuilder(
                                String.format(
                                        "JSON_VALUE(%s, %s RETURNING %s ",
                                        operands.get(0).asSerializableString(),
                                        operands.get(1).asSerializableString(),
                                        operands.get(2).asSerializableString()));

                final JsonValueOnEmptyOrError onEmpty =
                        getSymbolLiteral(operands.get(3), JsonValueOnEmptyOrError.class);

                if (onEmpty == JsonValueOnEmptyOrError.DEFAULT) {
                    s.append(String.format("DEFAULT %s", operands.get(4).asSerializableString()));
                } else {
                    s.append(onEmpty);
                }
                s.append(" ON EMPTY ");

                final JsonValueOnEmptyOrError onError =
                        getSymbolLiteral(operands.get(5), JsonValueOnEmptyOrError.class);

                if (onError == JsonValueOnEmptyOrError.DEFAULT) {
                    s.append(String.format("DEFAULT %s", operands.get(6).asSerializableString()));
                } else {
                    s.append(onError);
                }
                s.append(" ON ERROR)");

                return s.toString();
            };

    static final SqlCallSyntax JSON_EXISTS =
            (sqlName, operands) -> {
                if (operands.size() == 3) {
                    return String.format(
                            "%s(%s, %s %s ON ERROR)",
                            sqlName,
                            operands.get(0).asSerializableString(),
                            operands.get(1).asSerializableString(),
                            getSymbolLiteral(operands.get(2), JsonExistsOnError.class));
                } else {
                    return SqlCallSyntax.FUNCTION.unparse(sqlName, operands);
                }
            };

    static final SqlCallSyntax JSON_QUERY =
            (sqlName, operands) -> {
                final JsonQueryWrapper wrapper =
                        getSymbolLiteral(operands.get(3), JsonQueryWrapper.class);
                final JsonQueryOnEmptyOrError onEmpty =
                        getSymbolLiteral(operands.get(4), JsonQueryOnEmptyOrError.class);
                final JsonQueryOnEmptyOrError onError =
                        getSymbolLiteral(operands.get(5), JsonQueryOnEmptyOrError.class);

                return String.format(
                        "JSON_QUERY(%s, %s RETURNING %s %s WRAPPER %s ON EMPTY %s ON ERROR)",
                        operands.get(0).asSerializableString(),
                        operands.get(1).asSerializableString(),
                        operands.get(2).asSerializableString(),
                        toString(wrapper),
                        onEmpty.toString().replaceAll("_", " "),
                        onError.toString().replaceAll("_", " "));
            };

    static final SqlCallSyntax JSON_OBJECT =
            (sqlName, operands) -> {
                final String entries =
                        IntStream.range(0, operands.size() / 2)
                                .mapToObj(
                                        i ->
                                                String.format(
                                                        "KEY %s VALUE %s",
                                                        operands.get(2 * i + 1)
                                                                .asSerializableString(),
                                                        operands.get(2 * i + 2)
                                                                .asSerializableString()))
                                .collect(Collectors.joining(", "));

                final JsonOnNull onNull = getSymbolLiteral(operands.get(0), JsonOnNull.class);
                return String.format("JSON_OBJECT(%s %s ON NULL)", entries, onNull);
            };

    static final SqlCallSyntax JSON_ARRAY =
            (sqlName, operands) -> {
                if (operands.size() == 1) {
                    return "JSON_ARRAY()";
                }
                final String entries =
                        operands.subList(1, operands.size()).stream()
                                .map(ResolvedExpression::asSerializableString)
                                .collect(Collectors.joining(", "));

                final JsonOnNull onNull = getSymbolLiteral(operands.get(0), JsonOnNull.class);
                return String.format("JSON_ARRAY(%s %s ON NULL)", entries, onNull);
            };

    static SqlCallSyntax jsonArrayAgg(JsonOnNull onNull) {
        return (sqlName, operands) ->
                String.format(
                        "%s(%s %s ON NULL)",
                        sqlName, operands.get(0).asSerializableString(), onNull);
    }

    static SqlCallSyntax jsonObjectAgg(JsonOnNull onNull) {
        return (sqlName, operands) ->
                String.format(
                        "%s(KEY %s VALUE %s %s ON NULL)",
                        sqlName,
                        operands.get(0).asSerializableString(),
                        operands.get(1).asSerializableString(),
                        onNull);
    }

    private static String toString(JsonQueryWrapper wrapper) {
        final String wrapperStr;
        switch (wrapper) {
            case WITHOUT_ARRAY:
                wrapperStr = "WITHOUT ARRAY";
                break;
            case CONDITIONAL_ARRAY:
                wrapperStr = "WITH CONDITIONAL ARRAY";
                break;
            case UNCONDITIONAL_ARRAY:
                wrapperStr = "WITH UNCONDITIONAL ARRAY";
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + wrapper);
        }
        return wrapperStr;
    }

    private JsonFunctionsCallSyntax() {}
}
