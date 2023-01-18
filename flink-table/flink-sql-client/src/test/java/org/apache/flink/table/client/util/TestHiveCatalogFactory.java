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

package org.apache.flink.table.client.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A test factory that is the same as {@link HiveCatalogFactory} except returning a {@link
 * HiveCatalog} always with an embedded Hive metastore to test logic of {@link HiveCatalogFactory}.
 */
public class TestHiveCatalogFactory extends HiveCatalogFactory {
    public static final String ADDITIONAL_TEST_DATABASE = "additional_test_database";
    public static final String TEST_TABLE = "test_table";
    static final String TABLE_WITH_PARAMETERIZED_TYPES = "param_types_table";

    @Override
    public String factoryIdentifier() {
        return "hive-test";
    }

    @Override
    public Catalog createCatalog(Context context) {
        final Configuration configuration = Configuration.fromMap(context.getOptions());

        // Developers may already have their own production/testing hive-site.xml set in their
        // environment,
        // and Flink tests should avoid using those hive-site.xml.
        // Thus, explicitly create a testing HiveConf for unit tests here
        Catalog hiveCatalog =
                HiveTestUtils.createHiveCatalog(
                        context.getName(),
                        configuration.getString(HiveCatalogFactoryOptions.HIVE_VERSION));

        // Creates an additional database to test tableEnv.useDatabase() will switch current
        // database of the catalog
        hiveCatalog.open();
        try {
            hiveCatalog.createDatabase(
                    ADDITIONAL_TEST_DATABASE,
                    new CatalogDatabaseImpl(new HashMap<>(), null),
                    false);
            hiveCatalog.createTable(
                    new ObjectPath(ADDITIONAL_TEST_DATABASE, TEST_TABLE),
                    createResolvedTable(new String[] {"testcol"}, new DataType[] {DataTypes.INT()}),
                    false);
            // create a table to test parameterized types
            hiveCatalog.createTable(
                    new ObjectPath("default", TABLE_WITH_PARAMETERIZED_TYPES),
                    createResolvedTable(
                            new String[] {"dec", "ch", "vch"},
                            new DataType[] {
                                DataTypes.DECIMAL(10, 10), DataTypes.CHAR(5), DataTypes.VARCHAR(15)
                            }),
                    false);
        } catch (DatabaseAlreadyExistException
                | TableAlreadyExistException
                | DatabaseNotExistException e) {
            throw new CatalogException(e);
        }

        return hiveCatalog;
    }

    private ResolvedCatalogTable createResolvedTable(
            String[] fieldNames, DataType[] fieldDataTypes) {
        final Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);
        final CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromFields(fieldNames, fieldDataTypes).build(),
                        null,
                        Collections.emptyList(),
                        options);
        final List<Column> resolvedColumns =
                IntStream.range(0, fieldNames.length)
                        .mapToObj(i -> Column.physical(fieldNames[i], fieldDataTypes[i]))
                        .collect(Collectors.toList());
        return new ResolvedCatalogTable(
                origin, new ResolvedSchema(resolvedColumns, Collections.emptyList(), null));
    }
}
