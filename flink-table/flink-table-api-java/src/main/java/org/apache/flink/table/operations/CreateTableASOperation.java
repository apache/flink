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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Operation to describe a CREATE TABLE AS statement. */
@Internal
public class CreateTableASOperation implements ModifyOperation {

    private final CreateTableOperation createTableOperation;

    private final Map<String, String> sinkModifyStaticPartitions;
    private final QueryOperation sinkModifyQuery;
    private final boolean sinkModifyOverwrite;

    public CreateTableASOperation(
            CreateTableOperation createTableOperation,
            Map<String, String> sinkModifyStaticPartitions,
            QueryOperation sinkModifyQuery,
            boolean sinkModifyOverwrite) {
        this.createTableOperation = createTableOperation;
        this.sinkModifyStaticPartitions = sinkModifyStaticPartitions;
        this.sinkModifyQuery = sinkModifyQuery;
        this.sinkModifyOverwrite = sinkModifyOverwrite;
    }

    public CreateTableOperation getCreateTableOperation() {
        return createTableOperation;
    }

    public SinkModifyOperation toSinkModifyOperation(CatalogManager catalogManager) {
        return new SinkModifyOperation(
                catalogManager.getTableOrError(createTableOperation.getTableIdentifier()),
                sinkModifyQuery,
                sinkModifyStaticPartitions,
                null, // targetColumns
                sinkModifyOverwrite,
                Collections.emptyMap());
    }

    public StagedSinkModifyOperation toStagedSinkModifyOperation(
            ObjectIdentifier tableIdentifier,
            ResolvedCatalogTable catalogTable,
            Catalog catalog,
            DynamicTableSink dynamicTableSink) {
        return new StagedSinkModifyOperation(
                ContextResolvedTable.permanent(tableIdentifier, catalog, catalogTable),
                sinkModifyQuery,
                sinkModifyStaticPartitions,
                null, // targetColumns
                sinkModifyOverwrite,
                Collections.emptyMap(),
                dynamicTableSink);
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("catalogTable", getCreateTableOperation().getCatalogTable());
        params.put("identifier", getCreateTableOperation().getTableIdentifier());
        params.put("ignoreIfExists", getCreateTableOperation().isIgnoreIfExists());
        params.put("isTemporary", getCreateTableOperation().isTemporary());
        params.put("staticPartitions", sinkModifyStaticPartitions);
        params.put("overwrite", sinkModifyOverwrite);

        return OperationUtils.formatWithChildren(
                "CREATE TABLE AS",
                params,
                Collections.singletonList(sinkModifyQuery),
                Operation::asSummaryString);
    }

    @Override
    public QueryOperation getChild() {
        return sinkModifyQuery;
    }

    @Override
    public <T> T accept(ModifyOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
