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
import org.apache.flink.table.planner.codegen.calls.JsonParseReuse;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.apache.flink.table.types.logical.LogicalType;

import java.lang.reflect.Method;

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
        return GenerateUtils.generateCallIfArgsNotNull(
                ctx,
                returnType,
                operands,
                true,
                false,
                argTerms -> {
                    final String call =
                            generateCallOnParsedInput(
                                    ctx,
                                    operands,
                                    argTerms,
                                    BuiltInMethods.JSON_TYPE(),
                                    BuiltInMethods.JSON_TYPE_PATH());
                    return CodeGenUtils.BINARY_STRING() + ".fromString(" + call + ")";
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
        return GenerateUtils.generateCallIfArgsNotNull(
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
     */
    private static String generateCallOnParsedInput(
            CodeGeneratorContext ctx,
            Seq<GeneratedExpression> operands,
            Seq<String> argTerms,
            Method wholeDocument,
            Method withPath) {
        final String parsed = JsonParseReuse.parseSharedInput(ctx, operands).resultTerm();
        if (argTerms.length() == 1) {
            return CodeGenUtils.qualifyMethod(wholeDocument) + "(" + parsed + ")";
        }

        final String pathSpec = operands.apply(1).literalValue().get().toString();
        final boolean isPathDefinite = SqlJsonUtils.isPathDefinite(pathSpec);
        return CodeGenUtils.qualifyMethod(withPath)
                + "("
                + parsed
                + ", "
                + argTerms.apply(1)
                + ".toString(), "
                + isPathDefinite
                + ")";
    }
}
