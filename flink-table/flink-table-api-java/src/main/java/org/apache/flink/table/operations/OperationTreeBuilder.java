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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;

import java.util.List;

/** A builder for building {@link org.apache.flink.table.operations.Operation}. */
@PublicEvolving
public interface OperationTreeBuilder {

    QueryOperation project(List<Expression> projectList, QueryOperation child);

    QueryOperation project(
            List<Expression> projectList, QueryOperation child, boolean explicitAlias);

    QueryOperation project(
            List<Expression> projectList, QueryOperation child, List<OverWindow> overWindows);

    QueryOperation addColumns(
            boolean replaceIfExist, List<Expression> fieldLists, QueryOperation child);

    QueryOperation renameColumns(List<Expression> aliases, QueryOperation child);

    QueryOperation dropColumns(List<Expression> fieldLists, QueryOperation child);

    QueryOperation aggregate(
            List<Expression> groupingExpressions,
            List<Expression> aggregates,
            QueryOperation child);

    QueryOperation windowAggregate(
            List<Expression> groupingExpressions,
            GroupWindow window,
            List<Expression> windowProperties,
            List<Expression> aggregates,
            QueryOperation child);

    QueryOperation windowAggregate(
            List<Expression> groupingExpressions,
            GroupWindow window,
            List<Expression> windowProperties,
            Expression aggregateFunction,
            QueryOperation child);

    Expression resolveExpression(Expression expression, QueryOperation... tableOperation);

    QueryOperation sort(List<Expression> fields, QueryOperation child);

    QueryOperation limitWithOffset(int offset, QueryOperation child);

    QueryOperation limitWithFetch(int fetch, QueryOperation child);

    QueryOperation alias(List<Expression> fields, QueryOperation child);

    QueryOperation filter(Expression condition, QueryOperation child);

    QueryOperation distinct(QueryOperation child);

    QueryOperation minus(QueryOperation left, QueryOperation right, boolean all);

    QueryOperation intersect(QueryOperation left, QueryOperation right, boolean all);

    QueryOperation union(QueryOperation left, QueryOperation right, boolean all);

    QueryOperation map(Expression mapFunction, QueryOperation child);

    QueryOperation flatMap(Expression tableFunctionCall, QueryOperation child);

    QueryOperation aggregate(
            List<Expression> groupingExpressions, Expression aggregate, QueryOperation child);

    QueryOperation values(DataType rowType, Expression... expressions);

    QueryOperation values(Expression... expressions);

    QueryOperation tableAggregate(
            List<Expression> groupingExpressions,
            Expression tableAggFunction,
            QueryOperation child);

    QueryOperation windowTableAggregate(
            List<Expression> groupingExpressions,
            GroupWindow window,
            List<Expression> windowProperties,
            Expression tableAggFunction,
            QueryOperation child);
}
