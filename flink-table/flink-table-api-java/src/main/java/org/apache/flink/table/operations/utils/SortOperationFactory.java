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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver.PostResolverFactory;
import org.apache.flink.table.expressions.utils.ResolvedExpressionDefaultVisitor;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SortQueryOperation;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ORDERING;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.ORDER_ASC;

/** Utility class for creating a valid {@link SortQueryOperation} operation. */
@Internal
final class SortOperationFactory {

    /**
     * Creates a valid {@link SortQueryOperation}.
     *
     * <p><b>NOTE:</b> If the collation is not explicitly specified for an expression, the
     * expression is wrapped in a default ascending order. If no expression is specified, the result
     * is not sorted but only limited.
     *
     * @param orders expressions describing order
     * @param child relational expression on top of which to apply the sort operation
     * @param postResolverFactory factory for creating resolved expressions
     * @return valid sort operation
     */
    QueryOperation createSort(
            List<ResolvedExpression> orders,
            QueryOperation child,
            PostResolverFactory postResolverFactory) {
        final OrderWrapper orderWrapper = new OrderWrapper(postResolverFactory);

        List<ResolvedExpression> convertedOrders =
                orders.stream().map(f -> f.accept(orderWrapper)).collect(Collectors.toList());
        return new SortQueryOperation(convertedOrders, child);
    }

    /**
     * Creates a valid {@link SortQueryOperation} with offset (possibly merged into a preceding
     * {@link SortQueryOperation}).
     *
     * @param offset offset to start from
     * @param child relational expression on top of which to apply the sort operation
     * @param postResolverFactory factory for creating resolved expressions
     * @return valid sort operation with applied offset
     */
    QueryOperation createLimitWithOffset(
            int offset, QueryOperation child, PostResolverFactory postResolverFactory) {
        SortQueryOperation previousSort = validateAndGetChildSort(child, postResolverFactory);

        if (offset < 0) {
            throw new ValidationException("Offset should be greater or equal 0");
        }

        if (previousSort.getOffset() != -1) {
            throw new ValidationException("OFFSET already defined");
        }

        return new SortQueryOperation(previousSort.getOrder(), previousSort.getChild(), offset, -1);
    }

    /**
     * Creates a valid {@link SortQueryOperation} with fetch (possibly merged into a preceding
     * {@link SortQueryOperation}).
     *
     * @param fetch fetch to limit
     * @param child relational expression on top of which to apply the sort operation
     * @param postResolverFactory factory for creating resolved expressions
     * @return valid sort operation with applied offset
     */
    QueryOperation createLimitWithFetch(
            int fetch, QueryOperation child, PostResolverFactory postResolverFactory) {
        SortQueryOperation previousSort = validateAndGetChildSort(child, postResolverFactory);

        if (fetch < 0) {
            throw new ValidationException("Fetch should be greater or equal 0");
        }

        int offset = Math.max(previousSort.getOffset(), 0);

        return new SortQueryOperation(
                previousSort.getOrder(), previousSort.getChild(), offset, fetch);
    }

    private SortQueryOperation validateAndGetChildSort(
            QueryOperation child, PostResolverFactory postResolverFactory) {
        final SortQueryOperation previousSort;
        if (child instanceof SortQueryOperation) {
            previousSort = (SortQueryOperation) child;
        } else {
            previousSort =
                    (SortQueryOperation)
                            createSort(Collections.emptyList(), child, postResolverFactory);
        }

        if ((previousSort).getFetch() != -1) {
            throw new ValidationException("FETCH is already defined.");
        }

        return previousSort;
    }

    private static class OrderWrapper extends ResolvedExpressionDefaultVisitor<ResolvedExpression> {

        private PostResolverFactory postResolverFactory;

        OrderWrapper(PostResolverFactory postResolverFactory) {
            this.postResolverFactory = postResolverFactory;
        }

        @Override
        public ResolvedExpression visit(CallExpression call) {
            if (ORDERING.contains(call.getFunctionDefinition())) {
                return call;
            } else {
                return defaultMethod(call);
            }
        }

        @Override
        protected ResolvedExpression defaultMethod(ResolvedExpression expression) {
            return postResolverFactory.wrappingCall(ORDER_ASC, expression);
        }
    }
}
