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
import org.apache.flink.types.variant.Variant;

import java.lang.reflect.Method;

import scala.Option;
import scala.collection.Seq;

/** Utilities for the code generation of JSON functions. */
public final class JsonCodeGenUtils {

    private static final int PARSED_VALUE_INDEX = Integer.MIN_VALUE;
    private static final int PARSED_ERROR_INDEX = Integer.MIN_VALUE + 1;

    private JsonCodeGenUtils() {}

    /**
     * Generates {@code PARSE_JSON(str[, allowDuplicateKeys])} ({@code isTry == false}) or {@code
     * TRY_PARSE_JSON(...)} ({@code isTry == true}).
     *
     * <p>The input is parsed at most once per record for a given (input, allowDuplicateKeys) pair
     * and shared across all such calls; {@code PARSE_JSON} rethrows a parse error while {@code
     * TRY_PARSE_JSON} returns NULL.
     *
     * <p>For {@code PARSE_JSON(str)} the generated code looks like:
     *
     * <pre>{@code
     * // reusable members shared by every call on the same (input, allowDup):
     * Variant variantParsed$0;
     * Throwable variantParseError$0;
     * StringData variantParseInput$0;
     * java.lang.Boolean variantParseDup$0;
     *
     * private Variant parseVariant$0(StringData in, java.lang.Boolean allowDup) {
     *   if (in == variantParseInput$0 && java.util.Objects.equals(allowDup, variantParseDup$0)) {
     *     return variantParsed$0;
     *   }
     *   variantParseInput$0 = in;
     *   variantParseDup$0 = allowDup;
     *   variantParseError$0 = null;
     *   try {
     *     variantParsed$0 =
     *         SqlJsonUtils.parseJsonToVariant(in, java.lang.Boolean.TRUE.equals(allowDup));
     *   } catch (Exception e) {
     *     variantParsed$0 = null;
     *     variantParseError$0 = t;
     *   }
     *   return variantParsed$0;
     * }
     *
     * // per call site:
     * result$0 = null;
     * isNull$0 = true;
     * if (!inIsNull) {
     *   result$0 = parseVariant$0(in, null);
     *   if (variantParseError$0 != null) { // only emitted for PARSE_JSON, not TRY_PARSE_JSON
     *     org.apache.flink.util.ExceptionUtils.rethrow(variantParseError$0);
     *   }
     *   isNull$0 = result$0 == null;
     * }
     * }</pre>
     */
    public static GeneratedExpression generateParseJson(
            CodeGeneratorContext ctx,
            LogicalType returnType,
            Seq<GeneratedExpression> operands,
            boolean isTry) {
        final GeneratedExpression input = operands.apply(0);
        final GeneratedExpression allowDup = operands.length() > 1 ? operands.apply(1) : null;

        final String key =
                "variantParse:"
                        + input.resultTerm()
                        + ":"
                        + (allowDup == null ? "" : allowDup.resultTerm());

        final Option<GeneratedExpression> cachedValue =
                ctx.getReusableInputUnboxingExprs(key, PARSED_VALUE_INDEX);
        final Option<GeneratedExpression> cachedError =
                ctx.getReusableInputUnboxingExprs(key, PARSED_ERROR_INDEX);

        final String valueTerm;
        final String errorTerm;
        if (cachedValue.isDefined() && cachedError.isDefined()) {
            valueTerm = cachedValue.get().resultTerm();
            errorTerm = cachedError.get().resultTerm();
        } else {
            final String variantType = CodeGenUtils.className(Variant.class);
            final String inputType = CodeGenUtils.boxedTypeTermForType(input.resultType());
            final String parsed = CodeGenUtils.newName(ctx, "variantParsed");
            final String error = CodeGenUtils.newName(ctx, "variantParseError");
            final String lastInput = CodeGenUtils.newName(ctx, "variantParseInput");
            final String lastDup = CodeGenUtils.newName(ctx, "variantParseDup");
            final String method = CodeGenUtils.newName(ctx, "parseVariant");

            ctx.addReusableMember(variantType + " " + parsed + ";");
            ctx.addReusableMember("Throwable " + error + ";");
            ctx.addReusableMember(inputType + " " + lastInput + ";");
            ctx.addReusableMember("java.lang.Boolean " + lastDup + ";");
            ctx.addReusableMember(
                    "private "
                            + variantType
                            + " "
                            + method
                            + "("
                            + inputType
                            + " in, java.lang.Boolean allowDup) {\n"
                            + "  if (in == "
                            + lastInput
                            + " && java.util.Objects.equals(allowDup, "
                            + lastDup
                            + ")) {\n"
                            + "    return "
                            + parsed
                            + ";\n"
                            + "  }\n"
                            + "  "
                            + lastInput
                            + " = in;\n"
                            + "  "
                            + lastDup
                            + " = allowDup;\n"
                            + "  "
                            + error
                            + " = null;\n"
                            + "  try {\n"
                            + "    "
                            + parsed
                            + " = "
                            + CodeGenUtils.qualifyMethod(BuiltInMethods.PARSE_JSON())
                            + "(in, java.lang.Boolean.TRUE.equals(allowDup));\n"
                            + "  } catch (Exception e) {\n"
                            + "    "
                            + parsed
                            + " = null;\n"
                            + "    "
                            + error
                            + " = e;\n"
                            + "  }\n"
                            + "  return "
                            + parsed
                            + ";\n"
                            + "}\n");

            final String dupArg =
                    allowDup == null
                            ? "null"
                            : "("
                                    + allowDup.nullTerm()
                                    + ") ? (java.lang.Boolean) null : java.lang.Boolean.valueOf("
                                    + allowDup.resultTerm()
                                    + ")";
            valueTerm = method + "(" + input.resultTerm() + ", " + dupArg + ")";
            errorTerm = error;

            ctx.addReusableInputUnboxingExprs(
                    key,
                    PARSED_VALUE_INDEX,
                    new GeneratedExpression(
                            valueTerm,
                            GeneratedExpression.NEVER_NULL(),
                            GeneratedExpression.NO_CODE(),
                            null,
                            Option.<Object>empty()));
            ctx.addReusableInputUnboxingExprs(
                    key,
                    PARSED_ERROR_INDEX,
                    new GeneratedExpression(
                            error,
                            GeneratedExpression.NEVER_NULL(),
                            GeneratedExpression.NO_CODE(),
                            null,
                            Option.<Object>empty()));
        }

        final String resultTerm =
                ctx.addReusableLocalVariable(
                        CodeGenUtils.primitiveTypeTermForType(returnType), "result");
        final String nullTerm = ctx.addReusableLocalVariable("boolean", "isNull");

        final String rethrow =
                isTry
                        ? ""
                        : "  if ("
                                + errorTerm
                                + " != null) {\n"
                                + "    org.apache.flink.util.ExceptionUtils.rethrow("
                                + errorTerm
                                + ");\n"
                                + "  }\n";

        final String code =
                input.code()
                        + "\n"
                        + (allowDup == null ? "" : allowDup.code() + "\n")
                        + resultTerm
                        + " = null;\n"
                        + nullTerm
                        + " = true;\n"
                        + "if (!"
                        + input.nullTerm()
                        + ") {\n"
                        + "  "
                        + resultTerm
                        + " = "
                        + valueTerm
                        + ";\n"
                        + rethrow
                        + "  "
                        + nullTerm
                        + " = "
                        + resultTerm
                        + " == null;\n"
                        + "}\n";

        return new GeneratedExpression(
                resultTerm, nullTerm, code, returnType, Option.<Object>empty());
    }

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
