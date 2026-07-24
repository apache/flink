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

package org.apache.flink.table.jdbc.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.jdbc.FlinkDatabaseMetaData;
import org.apache.flink.table.jdbc.FlinkResultSet;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Utils to create catalog/schema results for {@link FlinkDatabaseMetaData}. */
public class DatabaseMetaDataUtils {
    private static final Column TABLE_CAT_COLUMN =
            Column.physical("TABLE_CAT", DataTypes.STRING().notNull());
    private static final Column TABLE_SCHEM_COLUMN =
            Column.physical("TABLE_SCHEM", DataTypes.STRING().notNull());
    private static final Column TABLE_CATALOG_COLUMN =
            Column.physical("TABLE_CATALOG", DataTypes.STRING());
    private static final Column TABLE_NAME_COLUMN =
            Column.physical("TABLE_NAME", DataTypes.STRING().notNull());
    private static final Column TABLE_TYPE_COLUMN =
            Column.physical("TABLE_TYPE", DataTypes.STRING().notNull());
    private static final Column REMARKS_COLUMN = Column.physical("REMARKS", DataTypes.STRING());
    private static final Column TYPE_CAT_COLUMN = Column.physical("TYPE_CAT", DataTypes.STRING());
    private static final Column TYPE_SCHEM_COLUMN =
            Column.physical("TYPE_SCHEM", DataTypes.STRING());
    private static final Column TYPE_NAME_COLUMN = Column.physical("TYPE_NAME", DataTypes.STRING());
    private static final Column SELF_REFERENCING_COL_NAME_COLUMN =
            Column.physical("SELF_REFERENCING_COL_NAME", DataTypes.STRING());
    private static final Column REF_GENERATION_COLUMN =
            Column.physical("REF_GENERATION", DataTypes.STRING());

    /**
     * Create result set for catalogs. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_CAT String => catalog name.
     * </ul>
     *
     * <p>The results are ordered by catalog name.
     *
     * @param statement The statement for database meta data
     * @param result The result for catalogs
     * @return a ResultSet object in which each row has a single String column that is a catalog
     *     name
     */
    public static FlinkResultSet createCatalogsResultSet(
            Statement statement, StatementResult result) {
        List<RowData> catalogs = new ArrayList<>();
        result.forEachRemaining(catalogs::add);
        catalogs.sort(Comparator.comparing(v -> v.getString(0)));

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(catalogs.iterator()),
                ResolvedSchema.of(TABLE_CAT_COLUMN));
    }

    /**
     * Create result set for schemas. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_SCHEM String => schema name
     *   <li>TABLE_CATALOG String => catalog name (may be null)
     * </ul>
     *
     * <p>The results are ordered by TABLE_CATALOG and TABLE_SCHEM.
     *
     * @param statement The statement for database meta data
     * @param catalogs The catalog list
     * @param catalogSchemas The catalog with schema list
     * @return a ResultSet object in which each row is a schema description
     */
    public static FlinkResultSet createSchemasResultSet(
            Statement statement, List<String> catalogs, Map<String, List<String>> catalogSchemas) {
        List<RowData> schemaWithCatalogList = new ArrayList<>();
        List<String> catalogList = new ArrayList<>(catalogs);
        catalogList.sort(String::compareTo);
        for (String catalog : catalogList) {
            List<String> schemas = catalogSchemas.get(catalog);
            schemas.sort(String::compareTo);
            schemas.forEach(
                    s ->
                            schemaWithCatalogList.add(
                                    GenericRowData.of(
                                            StringData.fromString(s),
                                            StringData.fromString(catalog))));
        }

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(schemaWithCatalogList.iterator()),
                ResolvedSchema.of(TABLE_SCHEM_COLUMN, TABLE_CATALOG_COLUMN));
    }

    /**
     * Create result set for tables. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_CAT String => table catalog (may be null)
     *   <li>TABLE_SCHEM String => table schema (may be null)
     *   <li>TABLE_NAME String => table name
     *   <li>TABLE_TYPE String => table type
     *   <li>REMARKS String => explanatory comment on the table
     *   <li>TYPE_CAT String => the types catalog (may be null)
     *   <li>TYPE_SCHEM String => the types schema (may be null)
     *   <li>TYPE_NAME String => type name (may be null)
     *   <li>SELF_REFERENCING_COL_NAME String => (may be null)
     *   <li>REF_GENERATION String => (may be null)
     * </ul>
     *
     * <p>The results are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, and TABLE_NAME.
     *
     * @param statement The statement for database meta data
     * @param tableRows The list of table row data
     * @return a ResultSet object in which each row is a table description
     */
    // JDBC getColumns() result columns
    private static final Column COLUMN_NAME_COLUMN =
            Column.physical("COLUMN_NAME", DataTypes.STRING().notNull());

    private static final Column DATA_TYPE_COLUMN =
            Column.physical("DATA_TYPE", DataTypes.INT().notNull());
    private static final Column COLUMN_TYPE_NAME_COLUMN =
            Column.physical("TYPE_NAME", DataTypes.STRING().notNull());
    private static final Column COLUMN_SIZE_COLUMN =
            Column.physical("COLUMN_SIZE", DataTypes.INT());
    private static final Column BUFFER_LENGTH_COLUMN =
            Column.physical("BUFFER_LENGTH", DataTypes.INT());
    private static final Column DECIMAL_DIGITS_COLUMN =
            Column.physical("DECIMAL_DIGITS", DataTypes.INT());
    private static final Column NUM_PREC_RADIX_COLUMN =
            Column.physical("NUM_PREC_RADIX", DataTypes.INT());
    private static final Column NULLABLE_COLUMN =
            Column.physical("NULLABLE", DataTypes.INT().notNull());
    private static final Column COLUMN_REMARKS_COLUMN =
            Column.physical("REMARKS", DataTypes.STRING());
    private static final Column COLUMN_DEF_COLUMN =
            Column.physical("COLUMN_DEF", DataTypes.STRING());
    private static final Column SQL_DATA_TYPE_COLUMN =
            Column.physical("SQL_DATA_TYPE", DataTypes.INT());
    private static final Column SQL_DATETIME_SUB_COLUMN =
            Column.physical("SQL_DATETIME_SUB", DataTypes.INT());
    private static final Column CHAR_OCTET_LENGTH_COLUMN =
            Column.physical("CHAR_OCTET_LENGTH", DataTypes.INT());
    private static final Column ORDINAL_POSITION_COLUMN =
            Column.physical("ORDINAL_POSITION", DataTypes.INT().notNull());
    private static final Column IS_NULLABLE_COLUMN =
            Column.physical("IS_NULLABLE", DataTypes.STRING().notNull());
    private static final Column SCOPE_CATALOG_COLUMN =
            Column.physical("SCOPE_CATALOG", DataTypes.STRING());
    private static final Column SCOPE_SCHEMA_COLUMN =
            Column.physical("SCOPE_SCHEMA", DataTypes.STRING());
    private static final Column SCOPE_TABLE_COLUMN =
            Column.physical("SCOPE_TABLE", DataTypes.STRING());
    private static final Column SOURCE_DATA_TYPE_COLUMN =
            Column.physical("SOURCE_DATA_TYPE", DataTypes.SMALLINT());
    private static final Column IS_AUTOINCREMENT_COLUMN =
            Column.physical("IS_AUTOINCREMENT", DataTypes.STRING().notNull());

    public static FlinkResultSet createColumnsResultSet(
            Statement statement, List<RowData> columnRows) {
        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(columnRows.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        COLUMN_NAME_COLUMN,
                        DATA_TYPE_COLUMN,
                        COLUMN_TYPE_NAME_COLUMN,
                        COLUMN_SIZE_COLUMN,
                        BUFFER_LENGTH_COLUMN,
                        DECIMAL_DIGITS_COLUMN,
                        NUM_PREC_RADIX_COLUMN,
                        NULLABLE_COLUMN,
                        COLUMN_REMARKS_COLUMN,
                        COLUMN_DEF_COLUMN,
                        SQL_DATA_TYPE_COLUMN,
                        SQL_DATETIME_SUB_COLUMN,
                        CHAR_OCTET_LENGTH_COLUMN,
                        ORDINAL_POSITION_COLUMN,
                        IS_NULLABLE_COLUMN,
                        SCOPE_CATALOG_COLUMN,
                        SCOPE_SCHEMA_COLUMN,
                        SCOPE_TABLE_COLUMN,
                        SOURCE_DATA_TYPE_COLUMN,
                        IS_AUTOINCREMENT_COLUMN));
    }

    public static FlinkResultSet createTablesResultSet(
            Statement statement, List<RowData> tableRows) {
        tableRows.sort(
                Comparator.comparing((RowData r) -> r.getString(3).toString())
                        .thenComparing(r -> r.getString(0).toString())
                        .thenComparing(r -> r.getString(1).toString())
                        .thenComparing(r -> r.getString(2).toString()));

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(tableRows.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        TABLE_TYPE_COLUMN,
                        REMARKS_COLUMN,
                        TYPE_CAT_COLUMN,
                        TYPE_SCHEM_COLUMN,
                        TYPE_NAME_COLUMN,
                        SELF_REFERENCING_COL_NAME_COLUMN,
                        REF_GENERATION_COLUMN));
    }

    // JDBC getPrimaryKeys() result columns
    private static final Column PK_NAME_COLUMN = Column.physical("PK_NAME", DataTypes.STRING());
    private static final Column KEY_SEQ_COLUMN =
            Column.physical("KEY_SEQ", DataTypes.INT().notNull());

    public static FlinkResultSet createPrimaryKeysResultSet(
            Statement statement, List<RowData> pkRows) {
        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(pkRows.iterator()),
                ResolvedSchema.of(
                        TABLE_CAT_COLUMN,
                        TABLE_SCHEM_COLUMN,
                        TABLE_NAME_COLUMN,
                        Column.physical("COLUMN_NAME", DataTypes.STRING().notNull()),
                        KEY_SEQ_COLUMN,
                        PK_NAME_COLUMN));
    }

    public static FlinkResultSet createTableTypesResultSet(
            Statement statement, List<RowData> typeRows) {
        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(typeRows.iterator()),
                ResolvedSchema.of(TABLE_TYPE_COLUMN));
    }
}
