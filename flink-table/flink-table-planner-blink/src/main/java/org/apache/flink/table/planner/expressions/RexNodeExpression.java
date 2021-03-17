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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Wrapper for a {@link ResolvedExpression} that originated from a {@link RexNode}.
 *
 * <p>If the {@link RexNode} was generated from a SQL expression, the expression can be made string
 * serializable and print the original SQL string as a summary.
 */
@Internal
public final class RexNodeExpression implements ResolvedExpression {

    private final RexNode rexNode;
    private final DataType outputDataType;
    private final @Nullable String summaryString;
    private final @Nullable String serializableString;

    public RexNodeExpression(
            RexNode rexNode,
            DataType outputDataType,
            @Nullable String summaryString,
            @Nullable String serializableString) {
        this.rexNode = checkNotNull(rexNode, "RexNode must not be null.");
        this.outputDataType = checkNotNull(outputDataType, "Output data type must not be null.");
        this.summaryString = summaryString;
        this.serializableString = serializableString;
    }

    public RexNode getRexNode() {
        return rexNode;
    }

    @Override
    public String asSummaryString() {
        if (summaryString != null) {
            return summaryString;
        }
        return rexNode.toString();
    }

    @Override
    public String asSerializableString() {
        if (serializableString != null) {
            return serializableString;
        }
        throw new TableException(
                String.format(
                        "Expression '%s' is not string serializable. Currently, only expressions that "
                                + "originated from a SQL expression have a well-defined string representation.",
                        asSummaryString()));
    }

    @Override
    public List<Expression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public DataType getOutputDataType() {
        return outputDataType;
    }

    @Override
    public List<ResolvedExpression> getResolvedChildren() {
        return new ArrayList<>();
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}
