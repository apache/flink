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

package org.apache.flink.table.planner.operations.converters.materializedtable;

import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlCreateMaterializedTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableMergeContext;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.MergeTableAsUtil;
import org.apache.flink.table.planner.operations.converters.SqlNodeConvertUtils;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The {@link MaterializedTableMergeContext} of a {@code CREATE [OR ALTER] MATERIALIZED TABLE}
 * statement.
 */
final class SqlMaterializedTableMergeContext implements MaterializedTableMergeContext {

    private final SqlCreateMaterializedTable sqlCreateMaterializedTable;
    private final ConvertContext context;
    private final MergeTableAsUtil mergeTableAsUtil;
    private final String originalQuery;
    private @Nullable PlannerQueryOperation asQueryOperation;

    SqlMaterializedTableMergeContext(
            SqlCreateMaterializedTable sqlCreateMaterializedTable, ConvertContext context) {
        this.sqlCreateMaterializedTable = sqlCreateMaterializedTable;
        this.context = context;
        this.mergeTableAsUtil = new MergeTableAsUtil(context);
        // Must run before validation, which rewrites the AS query node in place.
        this.originalQuery = deriveOriginalQuery(sqlCreateMaterializedTable, context);
    }

    @Override
    public boolean hasSchemaDefinition() {
        final SqlNodeList sqlNodeList = sqlCreateMaterializedTable.getColumnList();
        return !sqlNodeList.getList().isEmpty()
                && sqlNodeList.getList().get(0) instanceof SqlRegularColumn;
    }

    @Override
    public boolean hasConstraintDefinition() {
        return !sqlCreateMaterializedTable.getTableConstraints().isEmpty() || hasSchemaDefinition();
    }

    @Override
    public Schema getMergedSchema() {
        final SqlNodeList sqlNodeList = sqlCreateMaterializedTable.getColumnList();
        final ResolvedSchema querySchema = getQuerySchema();
        MaterializedTableUtils.validatePersistedColumnsUsedByQuery(
                sqlNodeList, querySchema, sqlCreateMaterializedTable.getOperator().getName());
        if (sqlCreateMaterializedTable.isSchemaWithColumnsIdentifiersOnly()) {
            // If only column identifiers are provided, then these are used to
            // order the columns in the schema.
            return mergeTableAsUtil.reorderSchema(sqlNodeList, querySchema);
        } else {
            return mergeTableAsUtil.mergeSchemas(
                    sqlNodeList,
                    sqlCreateMaterializedTable.getWatermark().orElse(null),
                    sqlCreateMaterializedTable.getFullConstraints(),
                    querySchema);
        }
    }

    @Override
    public ResolvedSchema getQuerySchema() {
        return asQueryOperation().getResolvedSchema();
    }

    @Override
    public Map<String, String> getOptions() {
        return sqlCreateMaterializedTable.getProperties();
    }

    @Override
    public List<String> getPartitionKeys() {
        return sqlCreateMaterializedTable.getPartitionKeyList();
    }

    @Override
    public Optional<TableDistribution> getDistribution() {
        return Optional.ofNullable(sqlCreateMaterializedTable.getDistribution())
                .map(OperationConverterUtils::getDistributionFromSqlDistribution);
    }

    @Override
    public @Nullable String getComment() {
        return sqlCreateMaterializedTable.getComment();
    }

    @Override
    public @Nullable IntervalFreshness getFreshness() {
        return Optional.ofNullable(sqlCreateMaterializedTable.getFreshness())
                .map(MaterializedTableUtils::getMaterializedTableFreshness)
                .orElse(null);
    }

    @Override
    public LogicalRefreshMode getLogicalRefreshMode() {
        return MaterializedTableUtils.deriveLogicalRefreshMode(
                sqlCreateMaterializedTable.getRefreshMode());
    }

    @Override
    public @Nullable StartMode getStartMode() {
        return MaterializedTableUtils.getStartMode(sqlCreateMaterializedTable.getStartMode());
    }

    @Override
    public String getOriginalQuery() {
        return originalQuery;
    }

    @Override
    public String getExpandedQuery() {
        final SqlNode selectQuery = sqlCreateMaterializedTable.getAsQuery();
        final SqlNode validatedQuery = context.getSqlValidator().validate(selectQuery);
        return context.expandSqlIdentifiers(context.toQuotedSqlString(validatedQuery));
    }

    @Override
    public QueryOperation getQueryOperation() {
        return asQueryOperation();
    }

    @Override
    public QueryOperation alignQuery(ResolvedCatalogMaterializedTable table) {
        return mergeTableAsUtil.maybeRewriteQuery(
                asQueryOperation(), sqlCreateMaterializedTable.getAsQuery(), table);
    }

    private PlannerQueryOperation asQueryOperation() {
        if (asQueryOperation == null) {
            asQueryOperation = toAsQueryOperation(sqlCreateMaterializedTable, context);
        }
        return asQueryOperation;
    }

    private static String deriveOriginalQuery(
            SqlCreateMaterializedTable sqlCreateMaterializedTable, ConvertContext context) {
        return SqlNodeConvertUtils.extractOriginalAsQueryText(
                        context, sqlCreateMaterializedTable.getAsQueryKeywordPos())
                .orElse(context.toQuotedSqlString(sqlCreateMaterializedTable.getAsQuery()));
    }

    private static PlannerQueryOperation toAsQueryOperation(
            SqlCreateMaterializedTable sqlCreateMaterializedTable, ConvertContext context) {
        final SqlNode selectQuery = sqlCreateMaterializedTable.getAsQuery();
        final SqlNode validateQuery = context.getSqlValidator().validate(selectQuery);
        return new PlannerQueryOperation(
                context.toRelRoot(validateQuery).project(),
                () -> context.toQuotedSqlString(validateQuery));
    }
}
