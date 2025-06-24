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
}
