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
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlFactory;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Describes a relational operation that was created from applying a (process) table function. */
@Internal
public class FunctionQueryOperation implements QueryOperation {

    private static final String INPUT_ALIAS = "$$T_FUNC";

    private final ContextResolvedFunction resolvedFunction;
    private final List<ResolvedExpression> arguments;
    private final ResolvedSchema resolvedSchema;

    public FunctionQueryOperation(
            ContextResolvedFunction resolvedFunction,
            List<ResolvedExpression> arguments,
            ResolvedSchema resolvedSchema) {
        this.resolvedFunction = resolvedFunction;
        this.arguments = arguments;
        this.resolvedSchema = resolvedSchema;
    }

    public ContextResolvedFunction getResolvedFunction() {
        return resolvedFunction;
    }

    public List<ResolvedExpression> getArguments() {
        return arguments;
    }

    public DataType getOutputDataType() {
        return DataTypeUtils.fromResolvedSchemaPreservingTimeAttributes(resolvedSchema);
    }

    @Override
    public String asSummaryString() {
        final Map<String, Object> args = new LinkedHashMap<>();
        args.put("function", resolvedFunction);
        args.put("arguments", arguments);

        return OperationUtils.formatWithChildren(
                "Function", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        return String.format(
                "SELECT %s FROM TABLE(%s\n) %s",
                OperationUtils.formatSelectColumns(getResolvedSchema(), INPUT_ALIAS),
                OperationUtils.indent(
                        resolvedFunction
                                .toCallExpression(arguments, resolvedSchema.toPhysicalRowDataType())
                                .asSerializableString(sqlFactory)),
                INPUT_ALIAS);
    }

    @Override
    public List<QueryOperation> getChildren() {
        return arguments.stream()
                .filter(TableReferenceExpression.class::isInstance)
                .map(TableReferenceExpression.class::cast)
                .map(TableReferenceExpression::getQueryOperation)
                .collect(Collectors.toList());
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
