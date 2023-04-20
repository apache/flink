/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.rex.RexInputRef;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A finder used to look up referenced column name in a {@link ResolvedExpression}. */
public class ColumnReferenceFinder {

    private ColumnReferenceFinder() {}

    /**
     * Find referenced column names that derive the computed column.
     *
     * @param columnName the name of the column
     * @param schema the schema contains the computed column definition
     * @return the referenced column names
     */
    public static Set<String> findReferencedColumn(String columnName, ResolvedSchema schema) {
        Column column =
                schema.getColumn(columnName)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "The input column %s doesn't exist in the schema.",
                                                        columnName)));
        if (!(column instanceof Column.ComputedColumn)) {
            return Collections.emptySet();
        }
        ColumnReferenceVisitor visitor =
                new ColumnReferenceVisitor(
                        // the input ref index is based on a projection of non-computed columns
                        schema.getColumns().stream()
                                .filter(c -> !(c instanceof Column.ComputedColumn))
                                .map(Column::getName)
                                .collect(Collectors.toList()));
        return visitor.visit(((Column.ComputedColumn) column).getExpression());
    }

    /**
     * Find referenced column names that derive the watermark expression.
     *
     * @param schema resolved columns contains the watermark expression.
     * @return the referenced column names
     */
    public static Set<String> findWatermarkReferencedColumn(ResolvedSchema schema) {
        ColumnReferenceVisitor visitor = new ColumnReferenceVisitor(schema.getColumnNames());
        return schema.getWatermarkSpecs().stream()
                .flatMap(
                        spec ->
                                Stream.concat(
                                        visitor.visit(spec.getWatermarkExpression()).stream(),
                                        Stream.of(spec.getRowtimeAttribute())))
                .collect(Collectors.toSet());
    }

    private static class ColumnReferenceVisitor extends ExpressionDefaultVisitor<Set<String>> {
        private final List<String> tableColumns;

        public ColumnReferenceVisitor(List<String> tableColumns) {
            this.tableColumns = tableColumns;
        }

        @Override
        public Set<String> visit(Expression expression) {
            if (expression instanceof LocalReferenceExpression) {
                return visit((LocalReferenceExpression) expression);
            } else if (expression instanceof FieldReferenceExpression) {
                return visit((FieldReferenceExpression) expression);
            } else if (expression instanceof RexNodeExpression) {
                return visit((RexNodeExpression) expression);
            } else if (expression instanceof CallExpression) {
                return visit((CallExpression) expression);
            } else {
                return super.visit(expression);
            }
        }

        @Override
        public Set<String> visit(FieldReferenceExpression fieldReference) {
            return Collections.singleton(fieldReference.getName());
        }

        public Set<String> visit(LocalReferenceExpression localReference) {
            return Collections.singleton(localReference.getName());
        }

        public Set<String> visit(RexNodeExpression rexNode) {
            // get the referenced column ref in table
            Set<RexInputRef> inputRefs = FlinkRexUtil.findAllInputRefs(rexNode.getRexNode());
            // get the referenced column name by index
            return inputRefs.stream()
                    .map(inputRef -> tableColumns.get(inputRef.getIndex()))
                    .collect(Collectors.toSet());
        }

        @Override
        public Set<String> visit(CallExpression call) {
            Set<String> references = new HashSet<>();
            for (Expression expression : call.getChildren()) {
                references.addAll(visit(expression));
            }
            return references;
        }

        @Override
        protected Set<String> defaultMethod(Expression expression) {
            throw new TableException("Unexpected expression: " + expression);
        }
    }
}
