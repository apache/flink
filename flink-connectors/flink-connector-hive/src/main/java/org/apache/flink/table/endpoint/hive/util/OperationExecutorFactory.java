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

package org.apache.flink.table.endpoint.hive.util;

import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import javax.annotation.Nullable;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_CATALOGS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_COLUMNS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_SCHEMAS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_TABLES_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_TABLE_TYPES_SCHEMA;
import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.EOS;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfo;

/** Factory to create the operation executor. */
public class OperationExecutorFactory {

    public static Callable<ResultSet> createGetCatalogsExecutor(
            SqlGatewayService service, SessionHandle sessionHandle) {
        return () -> executeGetCatalogs(service, sessionHandle);
    }

    public static Callable<ResultSet> createGetSchemasExecutor(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String schemaName) {
        return () -> executeGetSchemas(service, sessionHandle, catalogName, schemaName);
    }

    public static Callable<ResultSet> createGetTablesExecutor(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String schemaName,
            @Nullable String tableName,
            Set<TableKind> tableKinds) {
        return () ->
                executeGetTables(
                        service, sessionHandle, catalogName, schemaName, tableName, tableKinds);
    }

    public static Callable<ResultSet> createGetColumnsExecutor(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String schemaName,
            @Nullable String tableName,
            @Nullable String columnsName) {
        return () ->
                executeGetColumns(
                        service, sessionHandle, catalogName, schemaName, tableName, columnsName);
    }

    public static Callable<ResultSet> createGetTableTypes() {
        return OperationExecutorFactory::executeGetTableTypes;
    }

    // --------------------------------------------------------------------------------------------
    // Executors
    // --------------------------------------------------------------------------------------------

    private static ResultSet executeGetCatalogs(
            SqlGatewayService service, SessionHandle sessionHandle) {
        Set<String> catalogNames = service.listCatalogs(sessionHandle);
        return buildResultSet(
                GET_CATALOGS_SCHEMA,
                catalogNames.stream()
                        .map(OperationExecutorFactory::wrap)
                        .collect(Collectors.toList()));
    }

    private static ResultSet executeGetSchemas(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String schemaName) {
        String specifiedCatalogName =
                isNullOrEmpty(catalogName) ? service.getCurrentCatalog(sessionHandle) : catalogName;
        Set<String> databaseNames =
                filter(service.listDatabases(sessionHandle, specifiedCatalogName), schemaName);
        return buildResultSet(
                GET_SCHEMAS_SCHEMA,
                databaseNames.stream()
                        .map(name -> wrap(name, specifiedCatalogName))
                        .collect(Collectors.toList()));
    }

    private static ResultSet executeGetTables(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String schemaName,
            @Nullable String tableName,
            Set<TableKind> tableKinds) {
        Set<TableInfo> tableInfos = new HashSet<>();
        String specifiedCatalogName =
                isNullOrEmpty(catalogName) ? service.getCurrentCatalog(sessionHandle) : catalogName;
        for (String schema :
                filter(service.listDatabases(sessionHandle, specifiedCatalogName), schemaName)) {
            tableInfos.addAll(
                    filter(
                            service.listTables(
                                    sessionHandle, specifiedCatalogName, schema, tableKinds),
                            candidate -> candidate.getIdentifier().getObjectName(),
                            tableName));
        }
        return buildResultSet(
                GET_TABLES_SCHEMA,
                tableInfos.stream()
                        .map(
                                info ->
                                        wrap(
                                                info.getIdentifier().getCatalogName(),
                                                info.getIdentifier().getDatabaseName(),
                                                info.getIdentifier().getObjectName(),
                                                info.getTableKind().name(),
                                                // It requires to load the CatalogFunction from the
                                                // remote server, which is time wasted.
                                                "",
                                                null,
                                                null,
                                                null,
                                                null,
                                                null))
                        .collect(Collectors.toList()));
    }

    private static ResultSet executeGetColumns(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String schemaName,
            @Nullable String tableName,
            @Nullable String columnName) {
        String specifiedCatalogName =
                isNullOrEmpty(catalogName) ? service.getCurrentCatalog(sessionHandle) : catalogName;
        Set<String> schemaNames =
                filter(service.listDatabases(sessionHandle, specifiedCatalogName), schemaName);
        Set<TableKind> tableKinds = new HashSet<>(Arrays.asList(TableKind.TABLE, TableKind.VIEW));
        List<Object[]> rowData = new ArrayList<>();

        for (String schema : schemaNames) {
            Set<TableInfo> tableInfos =
                    filter(
                            service.listTables(
                                    sessionHandle, specifiedCatalogName, schema, tableKinds),
                            candidates -> candidates.getIdentifier().getObjectName(),
                            tableName);

            for (TableInfo tableInfo : tableInfos) {
                ResolvedCatalogBaseTable<?> table =
                        service.getTable(sessionHandle, tableInfo.getIdentifier());
                List<Column> columns = table.getResolvedSchema().getColumns();

                Set<String> requiredColumnNames =
                        filter(
                                new HashSet<>(table.getResolvedSchema().getColumnNames()),
                                columnName);
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    if (requiredColumnNames.contains(column.getName())) {
                        LogicalType flinkColumnType = column.getDataType().getLogicalType();
                        Type hiveColumnType =
                                Type.getType(
                                        HiveTypeUtil.toHiveTypeInfo(column.getDataType(), false));
                        rowData.add(
                                new Object[] {
                                    specifiedCatalogName, // TABLE_CAT
                                    tableInfo.getIdentifier().getDatabaseName(), // TABLE_SCHEMA
                                    tableInfo.getIdentifier().getObjectName(), // TABLE_NAME
                                    column.getName(), // COLUMN_NAME
                                    hiveColumnType.toJavaSQLType(), // DATA_TYPE
                                    hiveColumnType.getName(), // TYPE_NAME
                                    getColumnSize(hiveColumnType, flinkColumnType), // COLUMN_SIZE
                                    null, // BUFFER_LENGTH, unused
                                    getDecimalDigits(
                                            hiveColumnType, flinkColumnType), // DECIMAL_DIGITS
                                    hiveColumnType.getNumPrecRadix(), // NUM_PREC_RADIX
                                    flinkColumnType.isNullable()
                                            ? DatabaseMetaData.columnNullable
                                            : DatabaseMetaData.columnNoNulls, // NULLABLE
                                    column.getComment().orElse(""), // REMARKS
                                    null, // COLUMN_DEF
                                    null, // SQL_DATA_TYPE
                                    null, // SQL_DATETIME_SUB
                                    null, // CHAR_OCTET_LENGTH
                                    i + 1, // ORDINAL_POSITION
                                    flinkColumnType.isNullable() ? "YES" : "NO", // IS_NULLABLE
                                    null, // SCOPE_CATALOG
                                    null, // SCOPE_SCHEMA
                                    null, // SCOPE_TABLE
                                    null, // SOURCE_DATA_TYPE
                                    "NO", // IS_AUTO_INCREMENT
                                });
                    }
                }
            }
        }
        return buildResultSet(
                GET_COLUMNS_SCHEMA,
                rowData.stream().map(OperationExecutorFactory::wrap).collect(Collectors.toList()));
    }

    private static ResultSet executeGetTableTypes() {
        return buildResultSet(
                GET_TABLE_TYPES_SCHEMA,
                Arrays.stream(TableKind.values())
                        .map(kind -> wrap(kind.name()))
                        .collect(Collectors.toList()));
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static ResultSet buildResultSet(ResolvedSchema resolvedSchema, List<RowData> data) {
        return new ResultSet(EOS, null, resolvedSchema, data);
    }

    private static boolean isNullOrEmpty(@Nullable String input) {
        return input == null || input.isEmpty();
    }

    private static Set<String> filter(Set<String> candidates, @Nullable String pattern) {
        return filter(candidates, Function.identity(), pattern);
    }

    private static <T> Set<T> filter(
            Set<T> candidates, Function<T, String> featureGetter, @Nullable String pattern) {
        Pattern compiledPattern = convertNamePattern(pattern);
        return candidates.stream()
                .filter(
                        candidate ->
                                compiledPattern.matcher(featureGetter.apply(candidate)).matches())
                .collect(Collectors.toSet());
    }

    /**
     * Covert SQL 'like' pattern to a Java regular expression. Underscores (_) are converted to '.'
     * and percent signs (%) are converted to '.*'. Note: escape characters are removed.
     *
     * @param pattern the SQL pattern to convert.
     * @return the equivalent Java regular expression of the pattern.
     */
    private static Pattern convertNamePattern(@Nullable String pattern) {
        if ((pattern == null) || pattern.isEmpty()) {
            pattern = "%";
        }
        String wStr = ".*";
        return Pattern.compile(
                pattern.replaceAll("([^\\\\])%", "$1" + wStr)
                        .replaceAll("\\\\%", "%")
                        .replaceAll("^%", wStr)
                        .replaceAll("([^\\\\])_", "$1.")
                        .replaceAll("\\\\_", "_")
                        .replaceAll("^_", "."));
    }

    private static GenericRowData wrap(Object... elements) {
        Object[] pack = new Object[elements.length];
        for (int i = 0; i < elements.length; i++) {
            Object element = elements[i];
            if (element != null) {
                if (element instanceof String) {
                    pack[i] = StringData.fromString((String) element);
                } else if (element instanceof Integer) {
                    pack[i] = element;
                } else if (element instanceof Short) {
                    pack[i] = element;
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Can not wrap the element %s at index %s into RowData.",
                                    element, i));
                }
            }
        }
        return GenericRowData.of(pack);
    }

    /**
     * The column size for this type. For numeric data this is the maximum precision. For character
     * data this is the length in characters. For datetime types this is the length in characters of
     * the String representation (assuming the maximum allowed precision of the fractional seconds
     * component). For binary data this is the length in bytes. Null is returned for for data types
     * where the column size is not applicable.
     */
    // TODO
    private static Integer getColumnSize(Type hiveColumnType, LogicalType flinkColumnType) {
        if (hiveColumnType.isNumericType()) {
            // Exactly precision for DECIMAL_TYPE and maximum precision for others.
            return hiveColumnType == Type.DECIMAL_TYPE
                    ? ((DecimalType) flinkColumnType).getPrecision()
                    : hiveColumnType.getMaxPrecision();
        }
        switch (hiveColumnType) {
            case STRING_TYPE:
            case BINARY_TYPE:
                return Integer.MAX_VALUE;
            case CHAR_TYPE:
            case VARCHAR_TYPE:
                return TypeInfoUtils.getCharacterLengthForType(
                        getPrimitiveTypeInfo(hiveColumnType.getName()));
            case DATE_TYPE:
                return 10;
            case TIMESTAMP_TYPE:
                return 29;
                // case TIMESTAMPLOCALTZ_TYPE:
                // return 31;
                // 还是用flinkColumnType来实现？
            default:
                return null;
        }
    }

    /**
     * The number of fractional digits for this type. Null is returned for data types where this is
     * not applicable.
     */
    private static Integer getDecimalDigits(Type hiveColumnType, LogicalType flinkColumnType) {
        switch (hiveColumnType) {
            case BOOLEAN_TYPE:
            case TINYINT_TYPE:
            case SMALLINT_TYPE:
            case INT_TYPE:
            case BIGINT_TYPE:
                return 0;
            case FLOAT_TYPE:
                return 7;
            case DOUBLE_TYPE:
                return 15;
            case DECIMAL_TYPE:
                return ((DecimalType) flinkColumnType).getScale();
            case TIMESTAMP_TYPE:
                return 9;
            default:
                return null;
        }
    }
}
