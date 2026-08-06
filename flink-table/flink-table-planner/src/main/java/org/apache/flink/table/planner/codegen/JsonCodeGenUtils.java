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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.table.planner.codegen.calls.BuiltInMethods;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.apache.flink.table.types.logical.LogicalType;

import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;

/** Utilities shared across the code generation of JSON functions. */
public final class JsonCodeGenUtils {

    private JsonCodeGenUtils() {}

    /**
     * Generates {@code JSON_TYPE(jsonValue)} or {@code JSON_TYPE(jsonValue, path)}: the first
     * argument is parsed into a {@link SqlJsonUtils.JsonValueContext} and the type flag is read off
     * it (optionally at {@code path}), guarded so that a NULL argument yields a NULL result.
     *
     * <p>The parsed context is shared with the other JSON functions over the same input, so the
     * input is parsed only once per record.
     */
    public static GeneratedExpression generateJsonType(
            CodeGeneratorContext ctx, LogicalType returnType, Seq<GeneratedExpression> operands) {
        boolean hasPath = operands.length() == 2;
        return GenerateUtils.generateCallWithStmtIfArgsNotNull(
                ctx,
                returnType,
                operands,
                true,
                false,
                argTerms -> {
                    ParsedJson parsed = getOrCreateParsedJson(ctx, argTerms.head() + ".toString()");
                    String call =
                            hasPath
                                    ? CodeGenUtils.qualifyMethod(BuiltInMethods.JSON_TYPE_PATH())
                                            + "("
                                            + parsed.varName
                                            + ", "
                                            + argTerms.apply(1)
                                            + ".toString())"
                                    : CodeGenUtils.qualifyMethod(BuiltInMethods.JSON_TYPE())
                                            + "("
                                            + parsed.varName
                                            + ")";
                    String resultExpr = CodeGenUtils.BINARY_STRING() + ".fromString(" + call + ")";
                    return new Tuple2<>(parsed.parseCode, resultExpr);
                });
    }

    /**
     * Emits code that parses the given JSON {@code inputTerm} into a reusable {@link
     * SqlJsonUtils.JsonValueContext} member variable.
     *
     * <p>When multiple JSON functions share the same input expression, the parse statement is
     * emitted only once and the parsed context is reused across all of them.
     *
     * @param ctx the code generator context
     * @param inputTerm the term producing the JSON string to parse
     * @return the parsed-context variable name and the parse statement (empty if already parsed)
     */
    private static ParsedJson getOrCreateParsedJson(CodeGeneratorContext ctx, String inputTerm) {
        Option<GeneratedExpression> existing =
                ctx.getReusableInputUnboxingExprs(inputTerm, Integer.MIN_VALUE);
        if (existing.isDefined()) {
            return new ParsedJson(existing.get().resultTerm(), "");
        }

        String varName = CodeGenUtils.newName(ctx, "jsonParsed");
        String typeName = SqlJsonUtils.JsonValueContext.class.getName();
        ctx.addReusableMember(typeName + " " + varName + ";");

        ctx.addReusableInputUnboxingExprs(
                inputTerm,
                Integer.MIN_VALUE,
                new GeneratedExpression(varName, "false", "", null, Option.empty()));

        String parseCode =
                varName
                        + " = "
                        + CodeGenUtils.qualifyMethod(BuiltInMethods.JSON_PARSE())
                        + "("
                        + inputTerm
                        + ");";
        return new ParsedJson(varName, parseCode);
    }

    /** Holds the outcome of {@link #getOrCreateParsedJson}. */
    private static final class ParsedJson {
        private final String varName;
        private final String parseCode;

        ParsedJson(String varName, String parseCode) {
            this.varName = varName;
            this.parseCode = parseCode;
        }
    }
}
