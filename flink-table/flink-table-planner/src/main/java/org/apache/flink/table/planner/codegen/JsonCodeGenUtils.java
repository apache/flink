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

import java.lang.reflect.Method;

import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;

/** Utilities for the code generation of JSON functions. */
public final class JsonCodeGenUtils {

    private JsonCodeGenUtils() {}

    /**
     * Generates {@code JSON_TYPE(jsonValue)} or {@code JSON_TYPE(jsonValue, path)}.
     *
     * <p>The parsed context is shared with the other JSON functions over the same input, so the
     * input is parsed only once per record.
     */
    public static GeneratedExpression generateJsonType(
            CodeGeneratorContext ctx, LogicalType returnType, Seq<GeneratedExpression> operands) {
        return GenerateUtils.generateCallWithStmtIfArgsNotNull(
                ctx,
                returnType,
                operands,
                true,
                false,
                argTerms -> {
                    Tuple2<String, String> parsedCall =
                            generateCallOnParsedInput(
                                    ctx,
                                    operands,
                                    argTerms,
                                    BuiltInMethods.JSON_TYPE(),
                                    BuiltInMethods.JSON_TYPE_PATH());
                    String resultExpr =
                            CodeGenUtils.BINARY_STRING() + ".fromString(" + parsedCall._2() + ")";
                    return new Tuple2<>(parsedCall._1(), resultExpr);
                });
    }

    /**
     * Generates {@code JSON_LENGTH(jsonValue)} or {@code JSON_LENGTH(jsonValue, path)}.
     *
     * <p>The parsed context is shared with the other JSON functions over the same input, so the
     * input is parsed only once per record.
     */
    public static GeneratedExpression generateJsonLength(
            CodeGeneratorContext ctx, LogicalType returnType, Seq<GeneratedExpression> operands) {
        return GenerateUtils.generateCallWithStmtIfArgsNotNull(
                ctx,
                returnType,
                operands,
                true,
                false,
                argTerms ->
                        generateCallOnParsedInput(
                                ctx,
                                operands,
                                argTerms,
                                BuiltInMethods.JSON_LENGTH(),
                                BuiltInMethods.JSON_LENGTH_PATH()));
    }

    /**
     * Builds the call against the shared parsed input: the whole-document overload, or the path
     * overload with the {@code isPathDefinite} flag resolved from the path literal at plan time via
     * {@link SqlJsonUtils#isPathDefinite(String)}.
     *
     * @return the parse statement and the call expression
     */
    private static Tuple2<String, String> generateCallOnParsedInput(
            CodeGeneratorContext ctx,
            Seq<GeneratedExpression> operands,
            Seq<String> argTerms,
            Method wholeDocument,
            Method withPath) {
        final ParsedJson parsed = getOrCreateParsedJson(ctx, argTerms.head() + ".toString()");
        if (argTerms.length() == 1) {
            return new Tuple2<>(
                    parsed.parseCode,
                    CodeGenUtils.qualifyMethod(wholeDocument) + "(" + parsed.varName + ")");
        }

        final String pathSpec = operands.apply(1).literalValue().get().toString();
        final boolean isPathDefinite = SqlJsonUtils.isPathDefinite(pathSpec);
        return new Tuple2<>(
                parsed.parseCode,
                CodeGenUtils.qualifyMethod(withPath)
                        + "("
                        + parsed.varName
                        + ", "
                        + argTerms.apply(1)
                        + ".toString(), "
                        + isPathDefinite
                        + ")");
    }

    /**
     * Emits code that parses the given JSON {@code inputTerm} into a reusable {@link
     * SqlJsonUtils.JsonValueContext} member variable.
     *
     * @return the parsed-context variable name and the parse statement, empty if the same input was
     *     already parsed
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
