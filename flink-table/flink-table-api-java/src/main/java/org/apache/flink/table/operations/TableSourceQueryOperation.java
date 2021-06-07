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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.sources.TableSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Inline scan of a {@link TableSource}. Used only when a {@link org.apache.flink.table.api.Table}
 * was created from {@link
 * org.apache.flink.table.api.TableEnvironment#fromTableSource(TableSource)}.
 */
@Internal
public class TableSourceQueryOperation<T> implements QueryOperation {

    private final TableSource<T> tableSource;
    private final boolean isBatch;

    public TableSourceQueryOperation(TableSource<T> tableSource, boolean isBatch) {
        this.tableSource = tableSource;
        this.isBatch = isBatch;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        final TableSchema legacySchema = tableSource.getTableSchema();
        return ResolvedSchema.physical(
                legacySchema.getFieldNames(), legacySchema.getFieldDataTypes());
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new HashMap<>();
        args.put("fields", tableSource.getTableSchema().getFieldNames());

        return OperationUtils.formatWithChildren(
                "TableSource", args, getChildren(), Operation::asSummaryString);
    }

    public TableSource<T> getTableSource() {
        return tableSource;
    }

    public boolean isBatch() {
        return isBatch;
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(QueryOperationVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
