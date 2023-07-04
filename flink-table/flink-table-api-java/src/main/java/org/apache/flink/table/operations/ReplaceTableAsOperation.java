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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;

/** Operation to describe a [CREATE OR] REPLACE TABLE AS statement. */
@Internal
public class ReplaceTableAsOperation implements ModifyOperation, ExecutableOperation {

    private final CreateTableOperation createTableOperation;
    private final QueryOperation sinkModifyQuery;
    private final boolean isCreateOrReplace;

    public ReplaceTableAsOperation(
            CreateTableOperation createTableOperation,
            QueryOperation sinkModifyQuery,
            boolean isCreateOrReplace) {
        this.createTableOperation = createTableOperation;
        this.sinkModifyQuery = sinkModifyQuery;
        this.isCreateOrReplace = isCreateOrReplace;
    }

    @Override
    public QueryOperation getChild() {
        return sinkModifyQuery;
    }

    @Override
    public <T> T accept(ModifyOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public CreateTableOperation getCreateTableOperation() {
        return createTableOperation;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("catalogTable", getCreateTableOperation().getCatalogTable());
        params.put("identifier", getCreateTableOperation().getTableIdentifier());

        return OperationUtils.formatWithChildren(
                isCreateOrReplace ? "CREATE OR REPLACE TABLE AS" : "REPLACE TABLE AS",
                params,
                Collections.singletonList(sinkModifyQuery),
                Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        CatalogManager catalogManager = ctx.getCatalogManager();
        ObjectIdentifier tableIdentifier = createTableOperation.getTableIdentifier();
        Optional<Catalog> optionalCatalog =
                ctx.getCatalogManager().getCatalog(tableIdentifier.getCatalogName());
        ObjectPath objectPath = tableIdentifier.toObjectPath();

        Catalog catalog = optionalCatalog.get();
        // rtas drop table first, then create
        try {
            // if is create or replace statement will ignore table not exist
            catalog.dropTable(objectPath, isCreateOrReplace);
        } catch (TableNotExistException e) {
            throw new TableException(
                    String.format(
                            "The table %s to be replaced doesn't exist. You may want to use CREATE TABLE AS statement or CREATE OR REPLACE TABLE AS statement.",
                            tableIdentifier));
        }

        // first create table
        CreateTableOperation executableCreateTableOperation =
                new CreateTableOperation(
                        createTableOperation.getTableIdentifier(),
                        createTableOperation.getCatalogTable(),
                        false,
                        false);
        executableCreateTableOperation.execute(ctx);

        // then insert into sink
        SinkModifyOperation sinkModifyOperation =
                new SinkModifyOperation(
                        catalogManager.getTableOrError(createTableOperation.getTableIdentifier()),
                        sinkModifyQuery,
                        Collections.emptyMap(),
                        null, // targetColumns
                        false,
                        Collections.emptyMap());

        List<ModifyOperation> modifyOperations = Collections.singletonList(sinkModifyOperation);
        List<Transformation<?>> transformations = ctx.translate(modifyOperations);
        List<String> sinkIdentifierNames = extractSinkIdentifierNames(modifyOperations);
        TableResultInternal result = ctx.executeInternal(transformations, sinkIdentifierNames);
        if (ctx.getTableConfig().get(TABLE_DML_SYNC)) {
            try {
                result.await();
            } catch (InterruptedException | ExecutionException e) {
                result.getJobClient().ifPresent(JobClient::cancel);
                throw new TableException("Fail to wait execution finish.", e);
            }
        }
        return result;
    }

    /**
     * extract sink identifier names from {@link ModifyOperation}s and deduplicate them with {@link
     * #deduplicateSinkIdentifierNames(List)}.
     */
    private List<String> extractSinkIdentifierNames(List<ModifyOperation> operations) {
        List<String> tableNames = new ArrayList<>(operations.size());
        for (ModifyOperation operation : operations) {
            if (operation instanceof SinkModifyOperation) {
                String fullName =
                        ((SinkModifyOperation) operation)
                                .getContextResolvedTable()
                                .getIdentifier()
                                .asSummaryString();
                tableNames.add(fullName);
            } else {
                throw new UnsupportedOperationException("Unsupported operation: " + operation);
            }
        }
        return deduplicateSinkIdentifierNames(tableNames);
    }

    /**
     * Deduplicate sink identifier names. If there are multiple tables with the same name, an index
     * suffix will be added at the end of the name to ensure each name is unique.
     */
    private List<String> deduplicateSinkIdentifierNames(List<String> tableNames) {
        Map<String, Integer> tableNameToCount = new HashMap<>();
        for (String fullName : tableNames) {
            tableNameToCount.put(fullName, tableNameToCount.getOrDefault(fullName, 0) + 1);
        }

        Map<String, Integer> tableNameToIndex = new HashMap<>();
        return tableNames.stream()
                .map(
                        tableName -> {
                            if (tableNameToCount.get(tableName) == 1) {
                                return tableName;
                            } else {
                                Integer index = tableNameToIndex.getOrDefault(tableName, 0) + 1;
                                tableNameToIndex.put(tableName, index);
                                return tableName + "_" + index;
                            }
                        })
                .collect(Collectors.toList());
    }
}
