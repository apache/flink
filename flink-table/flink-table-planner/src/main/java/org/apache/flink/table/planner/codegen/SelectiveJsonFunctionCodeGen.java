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

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.functions.JsonPathReader;
import org.apache.flink.table.runtime.functions.JsonPathSpec;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Parses the input of {@code JSON_VALUE}, {@code JSON_QUERY}, {@code JSON_LENGTH} and {@code
 * JSON_TYPE} with a {@link JsonPathReader}, which builds only what their paths reach, instead of
 * the whole document. The calls evaluate their paths on the result as before.
 *
 * <p>An input is parsed this way only if every such call on it has a literal path of one or more
 * {@code .name}, {@code ['name']} and {@code [index]} steps after {@code $}.
 */
final class SelectiveJsonFunctionCodeGen {

    private final CodeGeneratorContext ctx;
    private final RexProgram program;

    // call on an input parsed by a reader -> that input
    private final Map<RexCall, Input> inputOfCall = new HashMap<>();

    SelectiveJsonFunctionCodeGen(CodeGeneratorContext ctx, RexProgram program) {
        this.ctx = ctx;
        this.program = program;
        analyze();
    }

    /** The term of the reader that parses the call's input, or null for the full parse. */
    @Nullable
    String readerOf(RexCall call) {
        final Input input = inputOfCall.get(call);
        if (input == null) {
            return null;
        }
        if (input.readerTerm == null) {
            input.readerTerm =
                    ctx.addReusableObject(new JsonPathReader(input.paths), "jsonPathReader", null);
        }
        return input.readerTerm;
    }

    /**
     * Groups the calls by input. An input gets a reader if each call on it reads a path of
     * properties and indices; a call without a path, or with any other path, needs the whole
     * document, so all calls on that input parse in full.
     */
    private void analyze() {
        // input local ref -> the calls on it and their steps
        final Map<Integer, Map<RexCall, Object[]>> calls = new LinkedHashMap<>();
        final Set<Integer> fullParses = new HashSet<>();
        for (RexNode expr : program.getExprList()) {
            if (!(expr instanceof RexCall)) {
                continue;
            }
            final RexCall call = (RexCall) expr;
            // JSON_LENGTH also takes a VARIANT, which the reader doesn't parse
            if (!isJsonFunction(call)
                    || !(call.getOperands().get(0) instanceof RexLocalRef)
                    || !SqlTypeUtil.inCharFamily(call.getOperands().get(0).getType())) {
                continue;
            }
            final int input = ((RexLocalRef) call.getOperands().get(0)).getIndex();
            final Object[] steps = stepsOf(call);
            // the whole document, which the reader would build in full as well
            if (steps == null || steps.length == 0) {
                fullParses.add(input);
            } else {
                calls.computeIfAbsent(input, ignored -> new LinkedHashMap<>()).put(call, steps);
            }
        }

        for (Map.Entry<Integer, Map<RexCall, Object[]>> entry : calls.entrySet()) {
            if (fullParses.contains(entry.getKey())) {
                continue;
            }
            final Input input = new Input(entry.getValue().values().toArray(new Object[0][]));
            for (RexCall call : entry.getValue().keySet()) {
                inputOfCall.put(call, input);
            }
        }
    }

    // the calls that share a parse of their input, see JsonParseReuse
    private static boolean isJsonFunction(RexCall call) {
        if (hasPathMode(call.getOperator())) {
            return true;
        }
        final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
        return definition == BuiltInFunctionDefinitions.JSON_LENGTH
                || definition == BuiltInFunctionDefinitions.JSON_TYPE;
    }

    // the steps of the call's path, empty without a path, or null for any other path
    @Nullable
    private Object[] stepsOf(RexCall call) {
        if (call.getOperands().size() == 1) {
            return new Object[0];
        }
        final String path = literalString(call.getOperands().get(1));
        if (path == null) {
            return null;
        }
        // read as the call reads it: JSON_LENGTH and JSON_TYPE pass a mode to JsonPath
        return hasPathMode(call.getOperator())
                ? new JsonPathSpec(path).steps()
                : JsonPathReader.steps(path);
    }

    // a path of these may start with lax or strict
    private static boolean hasPathMode(SqlOperator operator) {
        return operator == FlinkSqlOperatorTable.JSON_VALUE
                || operator == FlinkSqlOperatorTable.JSON_QUERY;
    }

    @Nullable
    private String literalString(RexNode node) {
        final RexLiteral literal = JsonCodeGenUtils.literalOf(program, node);
        if (literal == null || literal.isNull() || !SqlTypeUtil.inCharFamily(literal.getType())) {
            return null;
        }
        return literal.getValueAs(String.class);
    }

    /** An input parsed by one reader for all its calls. */
    private static final class Input {
        private final Object[][] paths;
        // set when the first call is generated
        @Nullable private String readerTerm;

        private Input(Object[][] paths) {
            this.paths = paths;
        }
    }
}
