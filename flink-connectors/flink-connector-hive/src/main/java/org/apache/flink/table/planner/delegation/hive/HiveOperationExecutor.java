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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.HiveInternalOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.ExecutableOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.HiveSetOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.delegation.hive.copy.HiveSetProcessor;
import org.apache.flink.table.planner.delegation.hive.operations.HiveLoadDataOperation;
import org.apache.flink.table.planner.delegation.hive.operations.HiveShowCreateTableOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A Hive's operation executor used to execute operation in custom way instead of Flink's
 * implementation.
 */
public class HiveOperationExecutor {

    private final ExecutableOperation.Context context;
    private final CatalogManager catalogManager;
    private final Map<String, String> hiveVariables;
    private final TableConfig tableConfig;

    public HiveOperationExecutor(ExecutableOperation.Context context) {
        this.context = context;
        this.catalogManager = context.getCatalogManager();
        this.tableConfig = context.getTableConfig();
        this.hiveVariables = tableConfig.get(HiveInternalOptions.HIVE_VARIABLES);
    }

    public Optional<TableResultInternal> executeOperation(Operation operation) {
        if (operation instanceof HiveSetOperation) {
            return executeHiveSetOperation((HiveSetOperation) operation);
        } else if (operation instanceof HiveLoadDataOperation) {
            return executeHiveLoadDataOperation((HiveLoadDataOperation) operation);
        } else if (operation instanceof HiveShowCreateTableOperation) {
            return executeShowCreateTableOperation((HiveShowCreateTableOperation) operation);
        } else if (operation instanceof DescribeTableOperation) {
            return executeDescribeTableOperation((DescribeTableOperation) operation);
        } else if (operation instanceof ExplainOperation) {
            ExplainOperation explainOperation = (ExplainOperation) operation;
            Preconditions.checkArgument(
                    explainOperation.getChild() instanceof HiveLoadDataOperation);
            return explainHiveLoadDataOperation(
                    (HiveLoadDataOperation) explainOperation.getChild());
        } else {
            throw new FlinkHiveException(
                    String.format(
                            "Unknown operation %s for HiveOperationExecutor.",
                            operation.getClass().getName()));
        }
    }

    private Optional<TableResultInternal> executeHiveSetOperation(
            HiveSetOperation hiveSetOperation) {
        Catalog currentCatalog =
                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).orElse(null);
        if (!(currentCatalog instanceof HiveCatalog)) {
            throw new FlinkHiveException(
                    "Only support SET command when the current catalog is HiveCatalog in Hive dialect.");
        }

        HiveConf hiveConf = ((HiveCatalog) currentCatalog).getHiveConf();

        if (!hiveSetOperation.getKey().isPresent() && !hiveSetOperation.getValue().isPresent()) {
            List<String> options;
            if (hiveSetOperation.isVerbose()) {
                // set -v
                options =
                        HiveSetProcessor.dumpOptions(
                                hiveConf.getAllProperties(), hiveConf, hiveVariables, tableConfig);
            } else {
                // set
                options =
                        HiveSetProcessor.dumpOptions(
                                hiveConf.getChangedProperties(),
                                hiveConf,
                                hiveVariables,
                                tableConfig);
            }
            return Optional.of(buildResultForShowVariable(options));
        } else {
            if (!hiveSetOperation.getValue().isPresent()) {
                // set key
                String option =
                        HiveSetProcessor.getVariable(
                                tableConfig.getConfiguration().toMap(),
                                hiveConf,
                                hiveVariables,
                                hiveSetOperation.getKey().get());
                return Optional.of(buildResultForShowVariable(Collections.singletonList(option)));
            } else {
                HiveSetProcessor.setVariable(
                        hiveConf,
                        hiveVariables,
                        hiveSetOperation.getKey().get(),
                        hiveSetOperation.getValue().get());
                return Optional.of(TableResultImpl.TABLE_RESULT_OK);
            }
        }
    }

    private TableResultInternal buildResultForShowVariable(List<String> results) {
        List<Row> rows = results.stream().map(Row::of).collect(Collectors.toList());
        return TableResultImpl.builder()
                .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                .schema(ResolvedSchema.of(Column.physical("variables", DataTypes.STRING())))
                .data(rows)
                .build();
    }

    private Optional<TableResultInternal> executeHiveLoadDataOperation(
            HiveLoadDataOperation hiveLoadDataOperation) {
        Catalog currentCatalog =
                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).orElse(null);
        if (!(currentCatalog instanceof HiveCatalog)) {
            throw new FlinkHiveException(
                    "Only support 'LOAD DATA INPATH' when the current catalog is HiveCatalog in Hive dialect.");
        }
        try {
            // Hive's loadTable/loadPartition will call method
            // SessionState.get().getCurrentDatabase(), so we have to start a session state
            HiveSessionState.startSessionState(
                    ((HiveCatalog) currentCatalog).getHiveConf(), catalogManager);
            HiveCatalog hiveCatalog = (HiveCatalog) currentCatalog;
            if (hiveLoadDataOperation.getPartitionSpec().size() > 0) {
                hiveCatalog.loadPartition(
                        hiveLoadDataOperation.getPath(),
                        hiveLoadDataOperation.getTablePath(),
                        hiveLoadDataOperation.getPartitionSpec(),
                        hiveLoadDataOperation.isOverwrite(),
                        hiveLoadDataOperation.isSrcLocal());
            } else {
                hiveCatalog.loadTable(
                        hiveLoadDataOperation.getPath(),
                        hiveLoadDataOperation.getTablePath(),
                        hiveLoadDataOperation.isOverwrite(),
                        hiveLoadDataOperation.isSrcLocal());
            }
            return Optional.of(TableResultImpl.TABLE_RESULT_OK);
        } finally {
            HiveSessionState.clearSessionState();
        }
    }

    private Optional<TableResultInternal> explainHiveLoadDataOperation(
            HiveLoadDataOperation hiveLoadDataOperation) {
        // get the plan for the partition part
        String partitionExplain = "";
        Map<String, String> partitionSpec = hiveLoadDataOperation.getPartitionSpec();
        if (!partitionSpec.isEmpty()) {
            String[] pv = new String[partitionSpec.size()];
            int i = 0;
            for (Map.Entry<String, String> partition : partitionSpec.entrySet()) {
                pv[i++] = String.format("%s=%s", partition.getKey(), partition.getValue());
            }
            partitionExplain = String.format(", partition=[%s]", String.join(", ", pv));
        }
        // construct the full plan
        String plan =
                String.format(
                        "LoadData(filepath=[%s], "
                                + "table=[%s],"
                                + " overwrite=[%s], local=[%s]%s)",
                        hiveLoadDataOperation.getPath(),
                        hiveLoadDataOperation.getTablePath(),
                        hiveLoadDataOperation.isOverwrite(),
                        hiveLoadDataOperation.isSrcLocal(),
                        partitionExplain);

        String explanation =
                "== Abstract Syntax Tree =="
                        + System.lineSeparator()
                        + plan
                        + System.lineSeparator()
                        + System.lineSeparator()
                        + "== Optimized Physical Plan =="
                        + System.lineSeparator()
                        + plan
                        + System.lineSeparator()
                        + System.lineSeparator()
                        + "== Optimized Execution Plan =="
                        + System.lineSeparator()
                        + plan
                        + System.lineSeparator();

        return Optional.of(
                TableResultImpl.builder()
                        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                        .schema(ResolvedSchema.of(Column.physical("result", DataTypes.STRING())))
                        .data(Collections.singletonList(Row.of(explanation)))
                        .build());
    }

    private Optional<TableResultInternal> executeShowCreateTableOperation(
            HiveShowCreateTableOperation showCreateTableOperation) {
        ObjectPath tablePath = showCreateTableOperation.getTablePath();
        Catalog currentCatalog =
                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).orElse(null);
        if (!(currentCatalog instanceof HiveCatalog)) {
            throw new FlinkHiveException(
                    "Only support 'SHOW CREATE TABLE' when the current catalog is HiveCatalog in Hive dialect.");
        }
        HiveCatalog hiveCatalog = (HiveCatalog) currentCatalog;
        HiveConf hiveConf = hiveCatalog.getHiveConf();
        Hive hive;
        Table tbl;
        try {
            hive = Hive.get(hiveConf);
            tbl = hive.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
        } catch (HiveException e) {
            throw new FlinkHiveException(String.format("Fail to get the table %s.", tablePath), e);
        }

        if (!HiveCatalog.isHiveTable(tbl.getTTable())) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The table %s to show isn't a Hive table,"
                                    + " but 'SHOW CREATE TABLE' only supports Hive table currently.",
                            tablePath));
        }

        String showCreateTableString = HiveShowTableUtils.showCreateTable(tablePath, tbl);
        TableResultInternal resultInternal =
                TableResultImpl.builder()
                        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                        .schema(ResolvedSchema.of(Column.physical("result", DataTypes.STRING())))
                        .data(Collections.singletonList(Row.of(showCreateTableString)))
                        .build();
        return Optional.of(resultInternal);
    }

    private Optional<TableResultInternal> executeDescribeTableOperation(
            DescribeTableOperation describeTableOperation) {
        // currently, if it's 'describe extended', we still delegate to Flink's own implementation
        if (describeTableOperation.isExtended()) {
            return Optional.of(describeTableOperation.execute(context));
        } else {
            ObjectIdentifier tableIdentifier = describeTableOperation.getSqlIdentifier();
            Catalog currentCatalog =
                    catalogManager.getCatalog(catalogManager.getCurrentCatalog()).orElse(null);
            if (!(currentCatalog instanceof HiveCatalog)) {
                // delegate to Flink's own implementation
                return Optional.of(describeTableOperation.execute(context));
            }
            HiveCatalog hiveCatalog = (HiveCatalog) currentCatalog;
            ObjectPath tablePath =
                    new ObjectPath(
                            tableIdentifier.getDatabaseName(), tableIdentifier.getObjectName());
            org.apache.hadoop.hive.metastore.api.Table table;
            try {
                table = hiveCatalog.getHiveTable(tablePath);
            } catch (TableNotExistException e) {
                throw new FlinkHiveException(
                        String.format(
                                "The table or view %s doesn't exist in catalog %s.",
                                tablePath, catalogManager.getCurrentCatalog()),
                        e);
            }
            if (!HiveCatalog.isHiveTable(table.getParameters())) {
                // if it's not a Hive table, delegate to Flink's own implementation
                return Optional.of(describeTableOperation.execute(context));
            }
            List<Row> result = new ArrayList<>();
            // describe table's columns
            List<FieldSchema> columns = table.getSd().getCols();
            List<FieldSchema> partitionColumns = table.getPartitionKeys();
            for (FieldSchema fieldSchema : columns) {
                result.add(describeColumn(fieldSchema));
            }
            for (FieldSchema fieldSchema : partitionColumns) {
                result.add(describeColumn(fieldSchema));
            }

            // table's partition information
            if (!partitionColumns.isEmpty()) {
                result.add(Row.of("# Partition Information", "", ""));
                for (FieldSchema fieldSchema : partitionColumns) {
                    result.add(describeColumn(fieldSchema));
                }
            }
            TableResultInternal tableResultInternal =
                    TableResultImpl.builder()
                            .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                            .schema(
                                    ResolvedSchema.physical(
                                            new String[] {"col_name", "data_type", "comment"},
                                            new DataType[] {
                                                DataTypes.STRING(),
                                                DataTypes.STRING(),
                                                DataTypes.STRING()
                                            }))
                            .data(result)
                            .build();
            return Optional.of(tableResultInternal);
        }
    }

    private Row describeColumn(FieldSchema fieldSchema) {
        return Row.of(
                fieldSchema.getName(),
                fieldSchema.getType(),
                fieldSchema.getComment() == null ? StringUtils.EMPTY : fieldSchema.getComment());
    }
}
