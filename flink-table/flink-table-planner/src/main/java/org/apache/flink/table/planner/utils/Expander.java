/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Utility that expand SQL identifiers in a SQL query.
 *
 * <p>Simple use:
 *
 * <blockquote>
 *
 * <code>
 * final String sql =<br>
 *     "select ename from emp where deptno &lt; 10";<br>
 * final Expander.Expanded expanded =<br>
 *     Expander.create(planner).expanded(sql);<br>
 * print(expanded); // "select `emp`.`ename` from `catalog`.`db`.`emp` where `emp`.`deptno` &lt; 10"
 * </code>
 *
 * </blockquote>
 *
 * <p>Calling {@link Expanded#toString()} generates a string that is similar to SQL where a user has
 * manually converted all identifiers as expanded, and which could then be persisted as expanded
 * query of a Catalog view.
 *
 * <p>For more advanced formatting, use {@link Expanded#substitute(Function)}.
 *
 * <p>Adjust {@link SqlParser.Config} to use a different parser or parsing options.
 */
public class Expander {
    private final FlinkPlannerImpl planner;

    private Expander(FlinkPlannerImpl planner) {
        this.planner = Objects.requireNonNull(planner);
    }

    /** Creates an Expander. * */
    public static Expander create(FlinkPlannerImpl planner) {
        return new Expander(planner);
    }

    /** Expands identifiers in a given SQL string, returning a {@link Expanded}. */
    public Expanded expanded(String ori) {
        final Map<SqlParserPos, SqlIdentifier> identifiers = new HashMap<>();
        final Map<String, SqlIdentifier> funcNameToId = new HashMap<>();
        final SqlNode oriNode = planner.parser().parse(ori);
        // parse again because validation is stateful, that means the node tree was probably
        // mutated.
        final SqlNode validated = planner.validate(planner.parser().parse(ori));
        validated.accept(
                new SqlBasicVisitor<Void>() {
                    @Override
                    public Void visit(SqlCall call) {
                        SqlOperator operator = call.getOperator();
                        if (operator instanceof BridgingSqlFunction) {
                            final SqlIdentifier functionID =
                                    ((BridgingSqlFunction) operator).getSqlIdentifier();
                            if (!functionID.isSimple()) {
                                funcNameToId.put(Util.last(functionID.names), functionID);
                            }
                        }
                        return super.visit(call);
                    }

                    @Override
                    public Void visit(SqlIdentifier identifier) {
                        // See SqlUtil#deriveAliasFromOrdinal, there is no good solution
                        // to distinguish between system alias (EXPR${number}) and user defines,
                        // and we stop expanding all of them.
                        if (!identifier.names.get(0).startsWith("EXPR$")) {
                            identifiers.putIfAbsent(identifier.getParserPosition(), identifier);
                        }
                        return null;
                    }
                });
        return new Expanded(oriNode, identifiers, funcNameToId);
    }

    /** Result of expanding. */
    public static class Expanded {
        private final SqlNode oriNode;
        private final Map<SqlParserPos, SqlIdentifier> identifiersMap;
        private final Map<String, SqlIdentifier> funcNameToId;

        Expanded(
                SqlNode oriNode,
                Map<SqlParserPos, SqlIdentifier> identifiers,
                Map<String, SqlIdentifier> funcNameToId) {
            this.oriNode = oriNode;
            this.identifiersMap = ImmutableMap.copyOf(identifiers);
            this.funcNameToId = ImmutableMap.copyOf(funcNameToId);
        }

        @Override
        public String toString() {
            return substitute(SqlNode::toString);
        }

        /**
         * Returns the SQL string with identifiers replaced according to the given unparse function.
         */
        public String substitute(Function<SqlNode, String> fn) {
            final SqlShuttle shuttle =
                    new SqlShuttle() {
                        @Override
                        public SqlNode visit(SqlCall call) {
                            SqlOperator operator = call.getOperator();
                            if (operator instanceof SqlUnresolvedFunction) {
                                final SqlUnresolvedFunction unresolvedFunction =
                                        (SqlUnresolvedFunction) operator;
                                final SqlIdentifier functionID =
                                        unresolvedFunction.getSqlIdentifier();
                                if (functionID.isSimple()
                                        && funcNameToId.containsKey(functionID.getSimple())) {
                                    SqlUnresolvedFunction newFunc =
                                            new SqlUnresolvedFunction(
                                                    funcNameToId.get(functionID.getSimple()),
                                                    unresolvedFunction.getReturnTypeInference(),
                                                    unresolvedFunction.getOperandTypeInference(),
                                                    unresolvedFunction.getOperandTypeChecker(),
                                                    unresolvedFunction.getParamTypes(),
                                                    unresolvedFunction.getFunctionType());
                                    return newFunc.createCall(
                                            call.getFunctionQuantifier(),
                                            call.getParserPosition(),
                                            call.getOperandList().toArray(new SqlNode[0]));
                                }
                            }
                            return super.visit(call);
                        }

                        @Override
                        public SqlNode visit(SqlIdentifier id) {
                            if (id.isStar()) {
                                return id;
                            }
                            final SqlIdentifier toReplace =
                                    identifiersMap.get(id.getParserPosition());
                            if (toReplace == null || id.names.size() >= toReplace.names.size()) {
                                return id;
                            }
                            return toReplace;
                        }
                    };
            final SqlNode substituted = this.oriNode.accept(shuttle);
            return fn.apply(substituted);
        }
    }
}
