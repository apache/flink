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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.parse.CalciteParser;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;

import java.util.stream.Stream;

/** Converts SQL expressions to {@link RexNode}. */
@Internal
public class SqlToRexConverter {

    private final FlinkPlannerImpl planner;

    private final SqlDialect sqlDialect;

    private final RelDataType inputRowType;

    private final @Nullable RelDataType outputType;

    public SqlToRexConverter(
            FlinkPlannerImpl planner,
            SqlDialect sqlDialect,
            RelDataType inputRowType,
            @Nullable RelDataType outputType) {
        this.planner = planner;
        this.sqlDialect = sqlDialect;
        this.inputRowType = inputRowType;
        this.outputType = outputType;
    }

    /**
     * Converts the given SQL expression string to an expanded string with fully qualified function
     * calls and escaped identifiers.
     *
     * <p>E.g. {@code my_udf(f0) + 1} to {@code `my_catalog`.`my_database`.`my_udf`(`f0`) + 1}
     */
    public String expand(String expr) {
        final CalciteParser parser = planner.parser();
        final SqlNode node = parser.parseExpression(expr);
        final SqlNode validated = planner.validateExpression(node, inputRowType, outputType);
        return validated.toSqlString(sqlDialect).getSql();
    }

    /**
     * Converts a SQL expression to a {@link RexNode} expression.
     *
     * @param expr SQL expression e.g. {@code `my_catalog`.`my_database`.`my_udf`(`f0`) + 1}
     */
    public RexNode convertToRexNode(String expr) {
        final CalciteParser parser = planner.parser();
        return planner.rex(parser.parseExpression(expr), inputRowType, outputType);
    }

    /** Converts a {@link SqlNode} to a {@link RexNode} expression. */
    public RexNode convertToRexNode(SqlNode sqlNode) {
        return planner.rex(sqlNode, inputRowType, outputType);
    }

    /**
     * Converts an array of SQL expressions to an array of {@link RexNode} expressions.
     *
     * @param exprs SQL expression e.g. {@code `my_catalog`.`my_database`.`my_udf`(`f0`) + 1}
     */
    public RexNode[] convertToRexNodes(String[] exprs) {
        final CalciteParser parser = planner.parser();
        return Stream.of(exprs)
                .map(parser::parseExpression)
                .map(node -> planner.rex(node, inputRowType, null))
                .toArray(RexNode[]::new);
    }
}
