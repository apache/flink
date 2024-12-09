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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a query operation from a {@link ContextResolvedTable}.
 *
 * <p>The source table is described by {@link #getContextResolvedTable()}, and in general is used
 * for every source which implementation is defined with {@link DynamicTableSource}. {@code
 * DataStream} sources are handled by {@code ExternalQueryOperation}.
 */
@Internal
public class SourceQueryOperation implements QueryOperation {

    private static final String INPUT_ALIAS = "$$T_SOURCE";
    private final ContextResolvedTable contextResolvedTable;
    private final @Nullable Map<String, String> dynamicOptions;

    public SourceQueryOperation(ContextResolvedTable contextResolvedTable) {
        this(contextResolvedTable, null);
    }

    public SourceQueryOperation(
            ContextResolvedTable contextResolvedTable,
            @Nullable Map<String, String> dynamicOptions) {
        this.contextResolvedTable = contextResolvedTable;
        this.dynamicOptions = dynamicOptions;
    }

    public ContextResolvedTable getContextResolvedTable() {
        return contextResolvedTable;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return contextResolvedTable.getResolvedSchema();
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("identifier", getContextResolvedTable().getIdentifier().asSummaryString());
        args.put("fields", getResolvedSchema().getColumnNames());
        if (dynamicOptions != null) {
            args.put("options", dynamicOptions);
        }

        return OperationUtils.formatWithChildren(
                "CatalogTable", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public String asSerializableString() {
        String s =
                String.format(
                        "SELECT %s FROM %s %s",
                        OperationUtils.formatSelectColumns(getResolvedSchema(), INPUT_ALIAS),
                        getContextResolvedTable().getIdentifier().asSerializableString(),
                        INPUT_ALIAS);

        if (dynamicOptions != null && !dynamicOptions.isEmpty()) {
            throw new TableException("Dynamic source options are not SQL serializable yet.");
        }

        return s;
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public @Nullable Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }
}
