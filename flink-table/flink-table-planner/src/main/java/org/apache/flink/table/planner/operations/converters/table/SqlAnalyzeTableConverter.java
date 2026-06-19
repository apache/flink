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

package org.apache.flink.table.planner.operations.converters.table;

import org.apache.flink.sql.parser.ddl.table.SqlAnalyzeTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AnalyzeTableOperation;
import org.apache.flink.table.operations.utils.ValidationUtils;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Convert ANALYZE TABLE statement. */
public class SqlAnalyzeTableConverter implements SqlNodeConverter<SqlAnalyzeTable> {
    @Override
    public Operation convertSqlNode(SqlAnalyzeTable analyzeTable, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(analyzeTable.fullTableName());
        CatalogManager catalogManager = context.getCatalogManager();
        ObjectIdentifier tableIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        if (optionalCatalogTable.isEmpty() || optionalCatalogTable.get().isTemporary()) {
            throw new ValidationException(
                    String.format(
                            "Table %s doesn't exist or is a temporary table.", tableIdentifier));
        }
        CatalogBaseTable baseTable = optionalCatalogTable.get().getResolvedTable();
        ValidationUtils.validateTableKind(
                baseTable, CatalogBaseTable.TableKind.TABLE, "analyze table");

        CatalogTable table = (CatalogTable) baseTable;
        ResolvedSchema schema =
                baseTable.getUnresolvedSchema().resolve(catalogManager.getSchemaResolver());

        LinkedHashMap<String, String> partitions = analyzeTable.getPartitions();
        List<CatalogPartitionSpec> targetPartitionSpecs = null;
        if (table.isPartitioned()) {
            if (!partitions.keySet().equals(new HashSet<>(table.getPartitionKeys()))) {
                throw new ValidationException(
                        String.format(
                                "Invalid ANALYZE TABLE statement. For partition table, all partition keys should be specified explicitly. "
                                        + "The given partition keys: [%s] are not match the target partition keys: [%s].",
                                String.join(",", partitions.keySet()),
                                String.join(",", table.getPartitionKeys())));
            }

            try {
                targetPartitionSpecs =
                        getPartitionSpecs(tableIdentifier, schema, partitions, catalogManager);
            } catch (Exception e) {
                throw new ValidationException(e.getMessage(), e);
            }
        } else if (!partitions.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Invalid ANALYZE TABLE statement. Table: %s is not a partition table, while partition values are given.",
                            tableIdentifier));
        }

        List<String> columns = analyzeTable.getColumnNames();
        List<Column> targetColumns;
        if (analyzeTable.isAllColumns()) {
            Preconditions.checkArgument(columns.isEmpty());
            // computed column and metadata column will be ignored
            targetColumns =
                    schema.getColumns().stream()
                            .filter(Column::isPhysical)
                            .collect(Collectors.toList());
        } else if (!columns.isEmpty()) {
            targetColumns =
                    columns.stream()
                            .map(
                                    c -> {
                                        Optional<Column> colOpt = schema.getColumn(c);
                                        if (colOpt.isEmpty()) {
                                            throw new ValidationException(
                                                    String.format(
                                                            "Column: %s does not exist in the table: %s.",
                                                            c, tableIdentifier));
                                        }
                                        Column col = colOpt.get();
                                        if (col instanceof Column.ComputedColumn) {
                                            throw new ValidationException(
                                                    String.format(
                                                            "Column: %s is a computed column, ANALYZE TABLE does not support computed column.",
                                                            c));
                                        } else if (col instanceof Column.MetadataColumn) {
                                            throw new ValidationException(
                                                    String.format(
                                                            "Column: %s is a metadata column, ANALYZE TABLE does not support metadata column.",
                                                            c));
                                        } else if (col instanceof Column.PhysicalColumn) {
                                            return col;
                                        } else {
                                            throw new ValidationException(
                                                    "Unknown column class: "
                                                            + col.getClass().getSimpleName());
                                        }
                                    })
                            .collect(Collectors.toList());
        } else {
            targetColumns = Collections.emptyList();
        }

        return new AnalyzeTableOperation(tableIdentifier, targetPartitionSpecs, targetColumns);
    }

    private List<CatalogPartitionSpec> getPartitionSpecs(
            ObjectIdentifier tableIdentifier,
            ResolvedSchema schema,
            LinkedHashMap<String, String> partitions,
            CatalogManager catalogManager)
            throws TableNotPartitionedException, TableNotExistException {
        List<Expression> filters = new ArrayList<>();
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            if (entry.getValue() != null) {
                CallExpression call =
                        CallExpression.temporary(
                                FunctionIdentifier.of("="),
                                BuiltInFunctionDefinitions.EQUALS,
                                Arrays.asList(
                                        getPartitionKeyExpr(schema, entry.getKey()),
                                        getPartitionValueExpr(
                                                schema, entry.getKey(), entry.getValue())),
                                DataTypes.BOOLEAN());
                filters.add(call);
            }
        }
        if (filters.isEmpty()) {
            return catalogManager
                    .getCatalog(tableIdentifier.getCatalogName())
                    .get()
                    .listPartitions(tableIdentifier.toObjectPath());
        } else {
            return catalogManager
                    .getCatalog(tableIdentifier.getCatalogName())
                    .get()
                    .listPartitionsByFilter(tableIdentifier.toObjectPath(), filters);
        }
    }

    private FieldReferenceExpression getPartitionKeyExpr(
            ResolvedSchema schema, String partitionKey) {
        int fieldIndex = schema.getColumnNames().indexOf(partitionKey);
        if (fieldIndex < 0) {
            throw new ValidationException(
                    String.format(
                            "Partition: %s does not exist in the schema: %s",
                            partitionKey, schema.getColumnNames()));
        }
        return new FieldReferenceExpression(
                partitionKey, schema.getColumnDataTypes().get(fieldIndex), 0, fieldIndex);
    }

    private ValueLiteralExpression getPartitionValueExpr(
            ResolvedSchema schema, String partitionKey, String partitionValue) {
        int fieldIndex = schema.getColumnNames().indexOf(partitionKey);
        if (fieldIndex < 0) {
            throw new ValidationException(
                    String.format(
                            "Partition: %s does not exist in the schema: %s",
                            partitionKey, schema.getColumnNames()));
        }
        DataType dataType = schema.getColumnDataTypes().get(fieldIndex);
        if (partitionValue == null) {
            return new ValueLiteralExpression(null, dataType.nullable());
        }
        Object value;
        switch (dataType.getLogicalType().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                value = partitionValue;
                break;
            case TINYINT:
                value = Byte.valueOf(partitionValue);
                break;
            case SMALLINT:
                value = Short.valueOf(partitionValue);
                break;
            case INTEGER:
                value = Integer.valueOf(partitionValue);
                break;
            case BIGINT:
                value = Long.valueOf(partitionValue);
                break;
            case FLOAT:
                value = Float.valueOf(partitionValue);
                break;
            case DOUBLE:
                value = Double.valueOf(partitionValue);
                break;
            case DECIMAL:
                value = new BigDecimal(partitionValue);
                break;
            case DATE:
                value = Date.valueOf(partitionValue);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                value = Time.valueOf(partitionValue);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                value = Timestamp.valueOf(partitionValue);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported partition value type: " + dataType.getLogicalType());
        }
        return new ValueLiteralExpression(value, dataType.notNull());
    }
}
