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
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.rex.RexInputRef;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A finder used to look up referenced column name in a {@link ResolvedExpression}. */
public class ColumnReferenceFinder {

    private ColumnReferenceFinder() {}

    public static Set<String> findReferencedColumn(
            ResolvedExpression resolvedExpression, List<String> tableColumns) {
        ColumnReferenceVisitor visitor = new ColumnReferenceVisitor(tableColumns);
        visitor.visit(resolvedExpression);
        return visitor.referencedColumns;
    }

    private static class ColumnReferenceVisitor extends ExpressionDefaultVisitor<Void> {
        private final List<String> tableColumns;
        private final Set<String> referencedColumns;

        public ColumnReferenceVisitor(List<String> tableColumns) {
            this.tableColumns = tableColumns;
            this.referencedColumns = new HashSet<>();
        }

        @Override
        public Void visit(Expression expression) {
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
        public Void visit(FieldReferenceExpression fieldReference) {
            referencedColumns.add(fieldReference.getName());
            return null;
        }

        public Void visit(LocalReferenceExpression localReference) {
            referencedColumns.add(localReference.getName());
            return null;
        }

        public Void visit(RexNodeExpression rexNode) {
            // get the referenced column ref in table
            Set<RexInputRef> inputRefs = FlinkRexUtil.findAllInputRefs(rexNode.getRexNode());
            // get the referenced column name by index
            inputRefs.forEach(
                    inputRef -> {
                        int index = inputRef.getIndex();
                        referencedColumns.add(tableColumns.get(index));
                    });
            return null;
        }

        @Override
        public Void visit(CallExpression call) {
            for (Expression expression : call.getChildren()) {
                visit(expression);
            }
            return null;
        }

        @Override
        protected Void defaultMethod(Expression expression) {
            throw new TableException("Unexpected expression: " + expression);
        }
    }
}
