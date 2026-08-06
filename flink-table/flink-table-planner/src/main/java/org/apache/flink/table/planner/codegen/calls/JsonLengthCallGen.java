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

package org.apache.flink.table.planner.codegen.calls;

import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.apache.flink.table.types.logical.LogicalType;

import scala.Option;
import scala.collection.Seq;

/**
 * {@link CallGenerator} for {@code JSON_LENGTH}.
 *
 * <p>The JSON input is parsed into a reusable {@link SqlJsonUtils.JsonValueContext} that is shared
 * with other JSON functions operating on the same input, so the parse statement is emitted only
 * once. When a path argument is present the path-aware {@link BuiltInMethods#JSON_LENGTH_PATH}
 * overload is used, otherwise the whole-document {@link BuiltInMethods#JSON_LENGTH} overload.
 *
 * <p>The result is nullable: besides propagating a {@code NULL} argument, {@code JSON_LENGTH}
 * itself returns {@code NULL} for invalid JSON, a path that matches nothing, or a wildcard path
 * that matches two or more nodes.
 */
public class JsonLengthCallGen implements CallGenerator {

    @Override
    public GeneratedExpression generate(
            CodeGeneratorContext ctx, Seq<GeneratedExpression> operands, LogicalType returnType) {

        String inputTerm = operands.apply(0).resultTerm() + ".toString()";

        // Parse the JSON input into a reusable context. When multiple JSON functions share the
        // same input expression the parse statement is emitted only once and reused.
        Option<GeneratedExpression> existing =
                ctx.getReusableInputUnboxingExprs(inputTerm, Integer.MIN_VALUE);
        final String parsedVar;
        final String parseCode;
        if (existing.isDefined()) {
            parsedVar = existing.get().resultTerm();
            parseCode = "";
        } else {
            parsedVar = CodeGenUtils.newName(ctx, "jsonParsed");
            String typeName = SqlJsonUtils.JsonValueContext.class.getName();
            ctx.addReusableMember(typeName + " " + parsedVar + ";");
            ctx.addReusableInputUnboxingExprs(
                    inputTerm,
                    Integer.MIN_VALUE,
                    new GeneratedExpression(parsedVar, "false", "", null, Option.empty()));
            parseCode =
                    parsedVar
                            + " = "
                            + CodeGenUtils.qualifyMethod(BuiltInMethods.JSON_PARSE())
                            + "("
                            + inputTerm
                            + ");";
        }

        final String lengthCall;
        if (operands.length() > 1) {
            lengthCall =
                    CodeGenUtils.qualifyMethod(BuiltInMethods.JSON_LENGTH_PATH())
                            + "("
                            + parsedVar
                            + ", "
                            + operands.apply(1).resultTerm()
                            + ".toString())";
        } else {
            lengthCall =
                    CodeGenUtils.qualifyMethod(BuiltInMethods.JSON_LENGTH())
                            + "("
                            + parsedVar
                            + ")";
        }

        String resultTypeTerm = CodeGenUtils.boxedTypeTermForType(returnType);
        String defaultValue = CodeGenUtils.primitiveDefaultValue(returnType);
        String nullTerm = ctx.addReusableLocalVariable("boolean", "isNull");
        String resultTerm = ctx.addReusableLocalVariable(resultTypeTerm, "result");

        StringBuilder argsNull = new StringBuilder();
        StringBuilder argsCode = new StringBuilder();
        for (int i = 0; i < operands.length(); i++) {
            GeneratedExpression operand = operands.apply(i);
            if (i > 0) {
                argsNull.append(" || ");
            }
            argsNull.append(operand.nullTerm());
            argsCode.append(operand.code()).append("\n");
        }

        String code =
                argsCode
                        + nullTerm
                        + " = "
                        + argsNull
                        + ";\n"
                        + resultTerm
                        + " = "
                        + defaultValue
                        + ";\n"
                        + "if (!"
                        + nullTerm
                        + ") {\n"
                        + parseCode
                        + "\n"
                        + resultTerm
                        + " = "
                        + lengthCall
                        + ";\n"
                        + nullTerm
                        + " = ("
                        + resultTerm
                        + " == null);\n"
                        + "}\n";

        return new GeneratedExpression(resultTerm, nullTerm, code, returnType, Option.empty());
    }
}
