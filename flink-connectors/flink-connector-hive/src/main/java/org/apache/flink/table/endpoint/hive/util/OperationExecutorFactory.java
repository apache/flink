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

import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.hive.HiveFunction;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.results.FunctionInfo;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.ResultSetImpl;
import org.apache.flink.table.gateway.api.results.TableInfo;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hive.serde2.thrift.Type;

import javax.annotation.Nullable;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_CATALOGS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_COLUMNS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_FUNCTIONS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_PRIMARY_KEYS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_SCHEMAS_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_TABLES_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_TABLE_TYPES_SCHEMA;
import static org.apache.flink.table.endpoint.hive.HiveServer2Schemas.GET_TYPE_INFO_SCHEMA;
import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.EOS;
import static org.apache.hadoop.hive.serde2.thrift.Type.ARRAY_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.BIGINT_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.BINARY_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.BOOLEAN_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.CHAR_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.DATE_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.DECIMAL_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.DOUBLE_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.FLOAT_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.INTERVAL_DAY_TIME_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.INTERVAL_YEAR_MONTH_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.INT_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.MAP_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.NULL_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.SMALLINT_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.STRING_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.STRUCT_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.TIMESTAMP_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.TINYINT_TYPE;
import static org.apache.hadoop.hive.serde2.thrift.Type.VARCHAR_TYPE;

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

    public static Callable<ResultSet> createGetTableTypesExecutor() {
        return () ->
                buildResultSet(
                        GET_TABLE_TYPES_SCHEMA,
                        Arrays.stream(TableKind.values())
                                .map(kind -> wrap(kind.name()))
                                .collect(Collectors.toList()));
    }

    public static Callable<ResultSet> createGetPrimaryKeys(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String schemaName,
            @Nullable String tableName) {
        return () ->
                executeGetPrimaryKeys(service, sessionHandle, catalogName, schemaName, tableName);
    }

    public static Callable<ResultSet> createGetFunctionsExecutor(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String databasePattern,
            @Nullable String functionNamePattern) {
        return () ->
                executeGetFunctions(
                        service, sessionHandle, catalogName, databasePattern, functionNamePattern);
    }

    public static Callable<ResultSet> createGetTypeInfoExecutor() {
        return () ->
                buildResultSet(
                        GET_TYPE_INFO_SCHEMA,
                        getSupportedHiveType().stream()
                                .map(
                                        type ->
                                                wrap(
                                                        type.getName(), // TYPE_NAME
                                                        type.toJavaSQLType(), // DATA_TYPE
                                                        type.getMaxPrecision(), // PRECISION
                                                        type.getLiteralPrefix(), // LITERAL_PREFIX
                                                        type.getLiteralSuffix(), // LITERAL_SUFFIX
                                                        type.getCreateParams(), // CREATE_PARAMS
                                                        type.getNullable(), // NULLABLE
                                                        type.isCaseSensitive(), // CASE_SENSITIVE
                                                        type.getSearchable(), // SEARCHABLE
                                                        type
                                                                .isUnsignedAttribute(), // UNSIGNED_ATTRIBUTE
                                                        type.isFixedPrecScale(), // FIXED_PREC_SCALE
                                                        type.isAutoIncrement(), // AUTO_INCREMENT
                                                        type.getLocalizedName(), // LOCAL_TYPE_NAME
                                                        type.getMinimumScale(), // MINIMUM_SCALE
                                                        type.getMaximumScale(), // MAXIMUM_SCALE
                                                        null, // SQL_DATA_TYPE, unused
                                                        null, // SQL_DATETIME_SUB, unused
                                                        type.getNumPrecRadix() // NUM_PREC_RADIX
                                                        ))
                                .collect(Collectors.toList()));
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
                filterAndSort(
                        service.listDatabases(sessionHandle, specifiedCatalogName), schemaName);
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
        Set<TableInfo> tableInfos = new LinkedHashSet<>();
        Set<TableInfo> viewInfos = new LinkedHashSet<>();

        String specifiedCatalogName =
                isNullOrEmpty(catalogName) ? service.getCurrentCatalog(sessionHandle) : catalogName;
        for (String schema :
                filterAndSort(
                        service.listDatabases(sessionHandle, specifiedCatalogName), schemaName)) {
            for (TableInfo tableInfo :
                    filterAndSort(
                            service.listTables(
                                    sessionHandle, specifiedCatalogName, schema, tableKinds),
                            candidate -> candidate.getIdentifier().getObjectName(),
                            tableName)) {
                if (tableInfo.getTableKind().equals(TableKind.TABLE)) {
                    tableInfos.add(tableInfo);
                } else {
                    viewInfos.add(tableInfo);
                }
            }
        }
        return buildResultSet(
                GET_TABLES_SCHEMA,
                Stream.concat(tableInfos.stream(), viewInfos.stream())
                        .map(
                                info ->
                                        wrap(
                                                info.getIdentifier().getCatalogName(),
                                                info.getIdentifier().getDatabaseName(),
                                                info.getIdentifier().getObjectName(),
                                                info.getTableKind().name(),
                                                // It requires to load the CatalogTable from the
                                                // remote server, which is time wasted.
                                                null,
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
                filterAndSort(
                        service.listDatabases(sessionHandle, specifiedCatalogName), schemaName);
        Set<TableKind> tableKinds = new HashSet<>(Arrays.asList(TableKind.values()));

        List<RowData> results = new ArrayList<>();
        for (String schema : schemaNames) {
            Set<TableInfo> tableInfos =
                    filterAndSort(
                            service.listTables(
                                    sessionHandle, specifiedCatalogName, schema, tableKinds),
                            candidates -> candidates.getIdentifier().getObjectName(),
                            tableName);

            for (TableInfo tableInfo : tableInfos) {
                ResolvedCatalogBaseTable<?> table =
                        service.getTable(sessionHandle, tableInfo.getIdentifier());
                List<Column> columns = table.getResolvedSchema().getColumns();

                Set<String> matchedColumnNames =
                        filterAndSort(
                                new HashSet<>(table.getResolvedSchema().getColumnNames()),
                                columnName);
                for (int i = 0; i < columns.size(); i++) {
                    Column column = columns.get(i);
                    if (!matchedColumnNames.contains(column.getName())) {
                        continue;
                    }
                    Type hiveColumnType =
                            Type.getType(HiveTypeUtil.toHiveTypeInfo(column.getDataType(), false));
                    LogicalType flinkColumnType = column.getDataType().getLogicalType();
                    results.add(
                            wrap(
                                    specifiedCatalogName,
                                    tableInfo.getIdentifier().getDatabaseName(),
                                    tableInfo.getIdentifier().getObjectName(),
                                    column.getName(),
                                    hiveColumnType.toJavaSQLType(),
                                    hiveColumnType.getName(),
                                    getColumnSize(flinkColumnType),
                                    null, // BUFFER_LENGTH, unused
                                    getDecimalDigits(flinkColumnType),
                                    hiveColumnType.getNumPrecRadix(),
                                    flinkColumnType.isNullable()
                                            ? DatabaseMetaData.columnNullable
                                            : DatabaseMetaData.columnNoNulls,
                                    column.getComment().orElse(null),
                                    null, // COLUMN_DEF
                                    null, // SQL_DATA_TYPE
                                    null, // SQL_DATETIME_SUB
                                    null, // CHAR_OCTET_LENGTH
                                    i + 1,
                                    flinkColumnType.isNullable() ? "YES" : "NO",
                                    null, // SCOPE_CATALOG
                                    null, // SCOPE_SCHEMA
                                    null, // SCOPE_TABLE
                                    null, // SOURCE_DATA_TYPE
                                    "NO"));
                }
            }
        }
        return buildResultSet(GET_COLUMNS_SCHEMA, results);
    }

    private static ResultSet executeGetPrimaryKeys(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String schemaName,
            @Nullable String tableName) {
        String specifiedCatalogName =
                isNullOrEmpty(catalogName) ? service.getCurrentCatalog(sessionHandle) : catalogName;
        Set<String> schemaNames =
                filterAndSort(
                        service.listDatabases(sessionHandle, specifiedCatalogName), schemaName);
        List<RowData> primaryKeys = new ArrayList<>();

        for (String schema : schemaNames) {
            Set<TableInfo> tableInfos =
                    filterAndSort(
                            service.listTables(
                                    sessionHandle,
                                    specifiedCatalogName,
                                    schema,
                                    new HashSet<>(Arrays.asList(TableKind.values()))),
                            candidate -> candidate.getIdentifier().getObjectName(),
                            tableName);

            for (TableInfo tableInfo : tableInfos) {
                ResolvedCatalogBaseTable<?> table =
                        service.getTable(sessionHandle, tableInfo.getIdentifier());
                UniqueConstraint primaryKey =
                        table.getResolvedSchema().getPrimaryKey().orElse(null);
                if (primaryKey == null) {
                    continue;
                }

                for (int i = 0; i < primaryKey.getColumns().size(); i++) {
                    primaryKeys.add(
                            wrap(
                                    specifiedCatalogName,
                                    tableInfo.getIdentifier().getDatabaseName(),
                                    tableInfo.getIdentifier().getObjectName(),
                                    primaryKey.getColumns().get(i),
                                    i + 1,
                                    primaryKey.getName()));
                }
            }
        }
        return buildResultSet(GET_PRIMARY_KEYS_SCHEMA, primaryKeys);
    }

    private static ResultSet executeGetFunctions(
            SqlGatewayService service,
            SessionHandle sessionHandle,
            @Nullable String catalogName,
            @Nullable String databasePattern,
            @Nullable String functionNamePattern) {
        String specifiedCatalogName =
                isNullOrEmpty(catalogName) ? service.getCurrentCatalog(sessionHandle) : catalogName;

        Set<FunctionInfo> candidates = new HashSet<>();
        // Add user defined functions
        for (String databaseName :
                filterAndSort(
                        service.listDatabases(sessionHandle, specifiedCatalogName),
                        databasePattern)) {
            candidates.addAll(
                    service.listUserDefinedFunctions(
                            sessionHandle, specifiedCatalogName, databaseName));
        }
        // Add system functions
        if (isNullOrEmpty(catalogName) && isNullOrEmpty(databasePattern)) {
            candidates.addAll(service.listSystemFunctions(sessionHandle));
        }
        // Filter out unmatched functions
        Set<FunctionInfo> matchedFunctions =
                filterAndSort(
                        candidates,
                        candidate -> candidate.getIdentifier().getFunctionName(),
                        functionNamePattern);
        return buildResultSet(
                GET_FUNCTIONS_SCHEMA,
                // Sort
                matchedFunctions.stream()
                        .sorted(
                                Comparator.comparing(
                                        info ->
                                                info.getIdentifier()
                                                        .asSummaryString()
                                                        .toLowerCase()))
                        .map(
                                info ->
                                        wrap(
                                                info.getIdentifier()
                                                        .getIdentifier()
                                                        .map(ObjectIdentifier::getCatalogName)
                                                        .orElse(null), // FUNCTION_CAT
                                                info.getIdentifier()
                                                        .getIdentifier()
                                                        .map(ObjectIdentifier::getDatabaseName)
                                                        .orElse(null), // FUNCTION_SCHEM
                                                info.getIdentifier()
                                                        .getFunctionName(), // FUNCTION_NAME
                                                "", // REMARKS
                                                info.getKind()
                                                        .map(
                                                                OperationExecutorFactory
                                                                        ::toFunctionResult)
                                                        .orElse(
                                                                DatabaseMetaData
                                                                        .functionResultUnknown), // FUNCTION_TYPE
                                                // TODO: remove until Catalog listFunctions return
                                                // CatalogFunction
                                                getFunctionName(
                                                        service.getFunctionDefinition(
                                                                sessionHandle,
                                                                UnresolvedIdentifier.of(
                                                                        info.getIdentifier()
                                                                                .toList()))))) // SPECIFIC_NAME
                        .collect(Collectors.toList()));
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static boolean isNullOrEmpty(@Nullable String input) {
        return input == null || input.isEmpty();
    }

    private static Set<String> filterAndSort(Set<String> candidates, @Nullable String pattern) {
        return filterAndSort(candidates, Function.identity(), pattern);
    }

    private static <T> Set<T> filterAndSort(
            Set<T> candidates, Function<T, String> featureGetter, @Nullable String pattern) {
        Pattern compiledPattern = convertNamePattern(pattern);
        return candidates.stream()
                .filter(
                        candidate ->
                                compiledPattern.matcher(featureGetter.apply(candidate)).matches())
                .sorted(Comparator.comparing(v -> featureGetter.apply(v).toLowerCase()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
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
                } else if (element instanceof Boolean) {
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

    private static ResultSet buildResultSet(ResolvedSchema schema, List<RowData> data) {
        return new ResultSetImpl(
                EOS,
                null,
                schema,
                data,
                SIMPLE_ROW_DATA_TO_STRING_CONVERTER,
                false,
                null,
                ResultKind.SUCCESS_WITH_CONTENT);
    }

    private static List<Type> getSupportedHiveType() {
        return Collections.unmodifiableList(
                Arrays.asList(
                        NULL_TYPE,
                        BOOLEAN_TYPE,
                        STRING_TYPE,
                        BINARY_TYPE,
                        TINYINT_TYPE,
                        SMALLINT_TYPE,
                        INT_TYPE,
                        BIGINT_TYPE,
                        FLOAT_TYPE,
                        DOUBLE_TYPE,
                        DECIMAL_TYPE,
                        DATE_TYPE,
                        TIMESTAMP_TYPE,
                        ARRAY_TYPE,
                        MAP_TYPE,
                        STRUCT_TYPE,
                        CHAR_TYPE,
                        VARCHAR_TYPE,
                        INTERVAL_YEAR_MONTH_TYPE,
                        INTERVAL_DAY_TIME_TYPE));
    }

    private static int toFunctionResult(FunctionKind kind) {
        switch (kind) {
            case SCALAR:
            case AGGREGATE:
                return DatabaseMetaData.functionNoTable;
            case TABLE:
            case ASYNC_TABLE:
            case TABLE_AGGREGATE:
                return DatabaseMetaData.functionReturnsTable;
            case OTHER:
                return DatabaseMetaData.functionResultUnknown;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown function kind: %s.", kind));
        }
    }

    private static String getFunctionName(FunctionDefinition definition) {
        if (definition instanceof HiveFunction) {
            return ((HiveFunction<?>) definition).getFunctionWrapper().getUDFClassName();
        } else if (definition instanceof UserDefinedFunction) {
            return ((UserDefinedFunction) definition).functionIdentifier();
        } else if (definition instanceof TableFunctionDefinition) {
            return ((TableFunctionDefinition) definition).getTableFunction().functionIdentifier();
        } else if (definition instanceof ScalarFunctionDefinition) {
            return ((ScalarFunctionDefinition) definition).getScalarFunction().functionIdentifier();
        } else if (definition instanceof AggregateFunctionDefinition) {
            return ((AggregateFunctionDefinition) definition)
                    .getAggregateFunction()
                    .functionIdentifier();
        } else if (definition instanceof TableAggregateFunctionDefinition) {
            return ((TableAggregateFunctionDefinition) definition)
                    .getTableAggregateFunction()
                    .functionIdentifier();
        } else if (definition instanceof BuiltInFunctionDefinition) {
            BuiltInFunctionDefinition builtIn = (BuiltInFunctionDefinition) definition;
            return builtIn.getRuntimeClass().orElse(definition.getClass().getCanonicalName());
        } else {
            return definition.getClass().getCanonicalName();
        }
    }

    /**
     * The column size for this type. For numeric data this is the maximum precision. For character
     * data this is the length in characters. For datetime types this is the length in characters of
     * the String representation (assuming the maximum allowed precision of the fractional seconds
     * component). For binary data this is the length in bytes. Null is returned for data types
     * where the column size is not applicable.
     */
    private static @Nullable Integer getColumnSize(LogicalType columnType) {
        switch (columnType.getTypeRoot()) {
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
            case DATE:
                return 10;
            case BIGINT:
                return 19;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DECIMAL:
                return ((DecimalType) columnType).getScale();
            case VARCHAR:
            case BINARY:
                return Integer.MAX_VALUE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return 29;
            default:
                return null;
        }
    }

    /**
     * The number of fractional digits for this type. Null is returned for data types where this is
     * not applicable.
     */
    private static @Nullable Integer getDecimalDigits(LogicalType columnType) {
        switch (columnType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return 0;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DECIMAL:
                return ((DecimalType) columnType).getScale();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return 9;
            default:
                return null;
        }
    }
}
