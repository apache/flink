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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;

/** Planner expression resolver for {@link UnresolvedCallExpression}. */
public class CallExpressionResolver {

    private final ExpressionResolver resolver;

    public CallExpressionResolver(RelBuilder relBuilder) {
        FlinkContext context = unwrapContext(relBuilder.getCluster());
        this.resolver =
                ExpressionResolver.resolverFor(
                                context.getTableConfig(),
                                name -> Optional.empty(),
                                context.getFunctionCatalog()
                                        .asLookup(
                                                str -> {
                                                    throw new TableException(
                                                            "We should not need to lookup any expressions at this point");
                                                }),
                                context.getCatalogManager().getDataTypeFactory(),
                                (sqlExpression, inputRowType, outputType) -> {
                                    throw new TableException(
                                            "SQL expression parsing is not supported at this location.");
                                })
                        .build();
    }

    public ResolvedExpression resolve(Expression expression) {
        List<ResolvedExpression> resolved = resolver.resolve(Collections.singletonList(expression));
        Preconditions.checkArgument(resolved.size() == 1);
        return resolved.get(0);
    }
}
