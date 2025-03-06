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
import org.apache.flink.table.functions.TableFunction;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Describes a relational operation that was created from applying a {@link TableFunction}. */
@Internal
public class CalculatedQueryOperation implements QueryOperation {

    public static final String INPUT_ALIAS = "$$T_LAT";

    private final ContextResolvedFunction resolvedFunction;
    private final List<ResolvedExpression> arguments;
    private final ResolvedSchema resolvedSchema;

    public CalculatedQueryOperation(
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

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("function", resolvedFunction);
        args.put("arguments", arguments);

        return OperationUtils.formatWithChildren(
                "CalculatedTable", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public String asSerializableString() {
        // if we ever add multi-way join in JoinQueryOperation we need to sort out uniqueness of the
        // table name
        return String.format(
                "LATERAL TABLE(%s) %s(%s)",
                resolvedFunction
                        .toCallExpression(arguments, resolvedSchema.toPhysicalRowDataType())
                        .asSerializableString(),
                INPUT_ALIAS,
                OperationUtils.formatSelectColumns(resolvedSchema, null));
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <U> U accept(QueryOperationVisitor<U> visitor) {
        return visitor.visit(this);
    }
}
