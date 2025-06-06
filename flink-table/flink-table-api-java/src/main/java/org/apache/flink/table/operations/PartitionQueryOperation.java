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
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Describes a relational operation that was created from applying a partitioning. */
@Internal
public class PartitionQueryOperation implements QueryOperation {

    private final List<ResolvedExpression> partitionExpressions;
    private final QueryOperation child;

    public PartitionQueryOperation(
            List<ResolvedExpression> partitionExpressions, QueryOperation child) {
        this.partitionExpressions = partitionExpressions;
        this.child = child;
    }

    public int[] getPartitionKeys() {
        return partitionExpressions.stream()
                .map(FieldReferenceExpression.class::cast)
                .map(FieldReferenceExpression::getFieldIndex)
                .mapToInt(Integer::intValue)
                .toArray();
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        return String.format(
                "(%s\n) PARTITION BY (%s)",
                OperationUtils.indent(child.asSerializableString(sqlFactory)),
                partitionExpressions.stream()
                        .map(expr -> expr.asSerializableString(sqlFactory))
                        .collect(Collectors.joining(", ")));
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("partition", partitionExpressions);

        return OperationUtils.formatWithChildren(
                "Partition", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return child.getResolvedSchema();
    }

    @Override
    public List<QueryOperation> getChildren() {
        return List.of(child);
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
