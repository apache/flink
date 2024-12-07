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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.operations.utils.OperationExpressionsUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Table operation that computes new table using given {@link Expression}s from its input relational
 * operation.
 */
@Internal
public class ProjectQueryOperation implements QueryOperation {

    private static final String INPUT_ALIAS = "$$T_PROJECT";
    private final List<ResolvedExpression> projectList;
    private final QueryOperation child;
    private final ResolvedSchema resolvedSchema;

    public ProjectQueryOperation(
            List<ResolvedExpression> projectList,
            QueryOperation child,
            ResolvedSchema resolvedSchema) {
        this.projectList = projectList;
        this.child = child;
        this.resolvedSchema = resolvedSchema;
    }

    public List<ResolvedExpression> getProjectList() {
        return projectList;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("projections", projectList);

        return OperationUtils.formatWithChildren(
                "Project", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public String asSerializableString() {
        return String.format(
                "SELECT %s FROM (%s\n) " + INPUT_ALIAS,
                IntStream.range(0, projectList.size())
                        .mapToObj(this::alias)
                        .map(
                                expr ->
                                        OperationExpressionsUtils.scopeReferencesWithAlias(
                                                INPUT_ALIAS, expr))
                        .map(ResolvedExpression::asSerializableString)
                        .collect(Collectors.joining(", ")),
                OperationUtils.indent(child.asSerializableString()));
    }

    private ResolvedExpression alias(int index) {
        final ResolvedExpression expression = projectList.get(index);
        final String columnName = resolvedSchema.getColumnNames().get(index);
        if (OperationExpressionsUtils.extractName(expression)
                .map(n -> n.equals(columnName))
                .orElse(false)) {
            return expression;
        } else {
            return CallExpression.permanent(
                    BuiltInFunctionDefinitions.AS,
                    Arrays.asList(expression, new ValueLiteralExpression(columnName)),
                    expression.getOutputDataType());
        }
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.singletonList(child);
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
