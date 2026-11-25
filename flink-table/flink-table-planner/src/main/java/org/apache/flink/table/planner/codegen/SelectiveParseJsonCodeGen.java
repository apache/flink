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

import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.functions.VariantPathReader;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.utils.SimplePath;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.commons.lang3.ArrayUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decides which {@code PARSE_JSON} and {@code TRY_PARSE_JSON} calls parse with a {@link
 * VariantPathReader}, which builds only the parts of the document that the query reads.
 *
 * <p>A call qualifies if its result is only read at literal paths, given by {@code [key]}, {@code
 * [index]}, {@code VARIANT_GET} or {@code TRY_VARIANT_GET}, and each value read goes into a
 * built-in function whose result has no VARIANT, such as {@code CAST(... AS INT)}, {@code
 * JSON_STRING} or {@code IS NULL}. The value at a path is built in full, so such a function gets
 * the same result. Only a VARIANT itself would differ, by the field name dictionary of the parsed
 * part.
 *
 * <p>Each input and {@code allowDuplicateKeys} is one parse, shared by all its calls, such as
 * {@code PARSE_JSON(a)} and {@code TRY_PARSE_JSON(a)}. So all calls of one parse use the reader or
 * none does. Each parse is decided on its own: in {@code SELECT CAST(PARSE_JSON(a)['x'] AS INT),
 * JSON_STRING(PARSE_JSON(b))}, the parse of {@code a} uses the reader, and only the parse of {@code
 * b} needs the whole document.
 */
final class SelectiveParseJsonCodeGen {

    private final CodeGeneratorContext ctx;
    private final RexProgram program;

    // qualifying parse call -> its group
    private final Map<RexCall, Group> groupOfCall = new HashMap<>();

    SelectiveParseJsonCodeGen(CodeGeneratorContext ctx, RexProgram program) {
        this.ctx = ctx;
        this.program = program;
        analyze();
    }

    /** The term of the reader that parses the call's input, or null for the full parse. */
    @Nullable
    String readerOf(RexCall call) {
        final Group group = groupOfCall.get(call);
        if (group == null) {
            return null;
        }
        if (group.term == null) {
            final Object[][] paths =
                    group.steps.stream().map(List::toArray).toArray(Object[][]::new);
            // keeps no state, so all instances of the generated class can share it
            group.term =
                    ctx.addReusableObject(new VariantPathReader(paths), "variantPathReader", null);
        }
        return group.term;
    }

    /**
     * Decides, for each {@code PARSE_JSON} and {@code TRY_PARSE_JSON} call, whether it uses a
     * reader.
     *
     * <p>Calls with the same input and a literal {@code allowDuplicateKeys} form a group, because
     * they share one parse. The expressions are walked in order, following each parse result
     * through its uses:
     *
     * <ul>
     *   <li>{@code [key]}, {@code [index]}, or {@code VARIANT_GET} and {@code TRY_VARIANT_GET} with
     *       a literal path: adds steps to the path, as in {@code PARSE_JSON(s)['a'][1]}.
     *   <li>A built-in whose result has no VARIANT, such as {@code CAST(... AS INT)} or {@code
     *       JSON_STRING}: reads the value at the path, so the reader builds that path.
     *   <li>Anything else, such as returning the VARIANT or a step that isn't a literal: the group
     *       parses in full.
     * </ul>
     *
     * <p>A group uses a reader if it reads at least one path and nothing needs the full parse.
     */
    private void analyze() {
        final List<RexNode> exprs = program.getExprList();
        final int inputCount = program.getInputRowType().getFieldCount();
        // parse calls grouped by input and allowDuplicateKeys, the calls that share a parse
        final Map<List<Object>, Group> groups = new LinkedHashMap<>();
        // a parse or a navigation on it -> its group and the steps from the parse
        final Map<Integer, Chain> chains = new HashMap<>();

        for (int k = inputCount; k < exprs.size(); k++) {
            final RexNode expr = exprs.get(k);
            if (!(expr instanceof RexCall)) {
                continue;
            }
            final RexCall call = (RexCall) expr;
            final List<Object> key = parseKeyOf(call);
            if (key != null) {
                final Group group = groups.computeIfAbsent(key, ignored -> new Group());
                group.calls.add(call);
                chains.put(k, new Chain(group, new Object[0]));
                continue;
            }
            final List<RexNode> operands = call.getOperands();
            for (int i = 0; i < operands.size(); i++) {
                final Chain chain = chainOf(operands.get(i), chains);
                // a group that needs the full parse stays so, so its other uses don't matter
                if (chain == null || chain.group.full) {
                    continue;
                }
                // how the call uses a parse or a path: more steps, a read, or anything else
                if (i == 0 && isNavigation(call)) {
                    final Object[] steps = navigationSteps(call);
                    final LogicalType result = FlinkTypeFactory.toLogicalType(call.getType());
                    if (steps == null) {
                        // a step that isn't a literal
                        chain.group.full = true;
                    } else if (result.is(LogicalTypeRoot.VARIANT)) {
                        // the result is a VARIANT again, so its uses decide
                        chains.put(k, chain.then(steps));
                    } else if (readsValueOnly(call)) {
                        // such as VARIANT_GET(v, '$.a', 'INT')
                        read(chain.then(steps));
                    } else {
                        // such as VARIANT_GET(v, '$.a', 'ARRAY<VARIANT>')
                        chain.group.full = true;
                    }
                } else if (readsValueOnly(call)) {
                    read(chain);
                } else {
                    chain.group.full = true;
                }
            }
        }
        // a parse or a path in the output or the filter needs the whole value
        final List<RexNode> roots = new ArrayList<>(program.getProjectList());
        if (program.getCondition() != null) {
            roots.add(program.getCondition());
        }
        for (RexNode root : roots) {
            final Chain chain = chainOf(root, chains);
            if (chain != null) {
                chain.group.full = true;
            }
        }

        for (Group group : groups.values()) {
            if (!group.full && !group.steps.isEmpty()) {
                for (RexCall call : group.calls) {
                    groupOfCall.put(call, group);
                }
            }
        }
    }

    // the value at the steps is read, so only the path is built; the whole document is built
    // when there are no steps
    private static void read(Chain chain) {
        if (chain.steps.length > 0) {
            chain.group.steps.add(Arrays.asList(chain.steps));
        } else {
            chain.group.full = true;
        }
    }

    private static boolean isNavigation(RexCall call) {
        return call.getKind() == SqlKind.ITEM || isVariantGet(call);
    }

    private static boolean isVariantGet(RexCall call) {
        final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
        return definition == BuiltInFunctionDefinitions.VARIANT_GET
                || definition == BuiltInFunctionDefinitions.TRY_VARIANT_GET;
    }

    // the steps of [key], [index], or VARIANT_GET and TRY_VARIANT_GET with a literal path, or null
    @Nullable
    private Object[] navigationSteps(RexCall call) {
        if (call.getKind() == SqlKind.ITEM) {
            final Object step = literalStep(call.getOperands().get(1));
            return step == null ? null : new Object[] {step};
        }
        final RexNode path = expand(call.getOperands().get(1));
        if (!(path instanceof RexLiteral) || ((RexLiteral) path).isNull()) {
            return null;
        }
        final List<SimplePath.Step> steps;
        try {
            // as VARIANT_GET reads it, 0-based
            steps = SimplePath.parse(((RexLiteral) path).getValueAs(String.class)).getSteps();
        } catch (RuntimeException e) {
            return null;
        }
        return steps.stream()
                .map(step -> step.isField() ? step.getFieldName() : (Object) step.getIndex())
                .toArray();
    }

    // The group key of PARSE_JSON or TRY_PARSE_JSON with a literal allowDuplicateKeys, or null.
    @Nullable
    private List<Object> parseKeyOf(RexCall call) {
        final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
        if (definition != BuiltInFunctionDefinitions.PARSE_JSON
                && definition != BuiltInFunctionDefinitions.TRY_PARSE_JSON) {
            return null;
        }
        boolean allowDuplicateKeys = false;
        if (call.getOperands().size() > 1) {
            final RexNode argument = expand(call.getOperands().get(1));
            if (!(argument instanceof RexLiteral)) {
                return null;
            }
            // NULL means false, as at runtime
            allowDuplicateKeys =
                    Boolean.TRUE.equals(((RexLiteral) argument).getValueAs(Boolean.class));
        }
        final RexNode input = call.getOperands().get(0);
        final Object inputKey =
                input instanceof RexLocalRef ? ((RexLocalRef) input).getIndex() : input;
        return Arrays.asList(inputKey, allowDuplicateKeys);
    }

    // a literal [key] as a String, a literal [index] as a 0-based Integer, or null
    @Nullable
    private Object literalStep(RexNode key) {
        final RexNode node = expand(key);
        if (!(node instanceof RexLiteral) || ((RexLiteral) node).isNull()) {
            return null;
        }
        final RexLiteral literal = (RexLiteral) node;
        if (SqlTypeUtil.inCharFamily(literal.getType())) {
            return literal.getValueAs(String.class);
        }
        if (SqlTypeUtil.isIntType(literal.getType())) {
            final long index = literal.getValueAs(Long.class);
            // an index below 1 or too large is left to the full parse, which reports it
            return index >= 1 && index - 1 <= Integer.MAX_VALUE ? (int) (index - 1) : null;
        }
        return null;
    }

    // Whether the call only reads the value: a built-in whose result has no VARIANT. A VARIANT in
    // the result, such as in ARRAY<VARIANT>, would keep the parsed dictionary, and a user-defined
    // function could read the dictionary.
    private static boolean readsValueOnly(RexCall call) {
        return !hasVariant(call) && !isUserDefined(call.getOperator());
    }

    private static boolean hasVariant(RexCall call) {
        return LogicalTypeChecks.hasNested(
                FlinkTypeFactory.toLogicalType(call.getType()), t -> t.is(LogicalTypeRoot.VARIANT));
    }

    private static boolean isUserDefined(SqlOperator operator) {
        final FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(operator);
        if (definition != null) {
            return !(definition instanceof BuiltInFunctionDefinition);
        }
        // legacy user-defined functions
        return operator instanceof ScalarSqlFunction || operator instanceof TableSqlFunction;
    }

    @Nullable
    private static Chain chainOf(RexNode node, Map<Integer, Chain> chains) {
        return node instanceof RexLocalRef ? chains.get(((RexLocalRef) node).getIndex()) : null;
    }

    private RexNode expand(RexNode node) {
        return node instanceof RexLocalRef ? program.expandLocalRef((RexLocalRef) node) : node;
    }

    /** The parse calls that share a parse, and the paths read from them. */
    private static final class Group {
        private final List<RexCall> calls = new ArrayList<>();
        private final Set<List<Object>> steps = new LinkedHashSet<>();
        // some use needs the whole document
        private boolean full;
        // the reader's term, set when first generated
        @Nullable private String term;
    }

    /** The steps of a literal path from a parse to a local ref. */
    private static final class Chain {
        private final Group group;
        private final Object[] steps;

        private Chain(Group group, Object[] steps) {
            this.group = group;
            this.steps = steps;
        }

        private Chain then(Object[] more) {
            return new Chain(group, ArrayUtils.addAll(steps, more));
        }
    }
}
