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

import scala.Option;

/** Utilities shared across the code generation of JSON functions. */
public final class JsonCodeGenHelper {

    private JsonCodeGenHelper() {}

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
    public static ParsedJson getOrCreateParsedJson(CodeGeneratorContext ctx, String inputTerm) {
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
    public static final class ParsedJson {
        private final String varName;
        private final String parseCode;

        ParsedJson(String varName, String parseCode) {
            this.varName = varName;
            this.parseCode = parseCode;
        }

        /** Name of the variable holding the parsed {@link SqlJsonUtils.JsonValueContext}. */
        public String varName() {
            return varName;
        }

        /** Statement that assigns the parsed context, or an empty string if already parsed. */
        public String parseCode() {
            return parseCode;
        }
    }
}
