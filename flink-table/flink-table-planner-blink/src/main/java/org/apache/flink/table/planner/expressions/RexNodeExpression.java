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

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Dummy wrapper for expressions that were converted to RexNode in a different way. */
public class RexNodeExpression implements ResolvedExpression {

    private RexNode rexNode;
    private DataType outputDataType;

    public RexNodeExpression(RexNode rexNode, DataType outputDataType) {
        this.rexNode = rexNode;
        this.outputDataType = outputDataType;
    }

    public RexNode getRexNode() {
        return rexNode;
    }

    @Override
    public String asSummaryString() {
        return rexNode.toString();
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
}
