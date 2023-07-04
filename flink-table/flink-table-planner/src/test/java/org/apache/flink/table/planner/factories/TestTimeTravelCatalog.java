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

package org.apache.flink.table.planner.factories;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Test Catalog for testing time travel. */
public class TestTimeTravelCatalog extends GenericInMemoryCatalog {
    public TestTimeTravelCatalog(String name) {
        super(name);

        this.catalogs = new HashMap<>();
    }

    private Map<String, List<Tuple2<Long, CatalogTable>>> catalogs;

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        super.createTable(tablePath, table, ignoreIfExists);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        return super.getTable(tablePath);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath, long timestamp)
            throws TableNotExistException {

        List<Tuple2<Long, CatalogTable>> tables = catalogs.get(tablePath.getObjectName());

        Tuple2<Long, CatalogTable> table =
                tables.stream()
                        .filter(t -> t.f0 <= timestamp)
                        .max(Comparator.comparing(t -> t.f0))
                        .get();
        return table.f1;
    }

    /**
     * @param tableName Table name
     * @param schema Table schema of the table
     * @param properties Table properties to construct a table instance
     * @param timestamp The snapshot of the table
     */
    public void registerTable(
            String tableName, Schema schema, Map<String, String> properties, long timestamp) {
        CatalogTable catalogTable =
                CatalogTable.of(schema, "", Collections.emptyList(), properties);

        if (!catalogs.containsKey(tableName)) {
            catalogs.put(tableName, new ArrayList<>());
        }

        catalogs.get(tableName).add(Tuple2.of(timestamp, catalogTable));
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        return catalogs.keySet().stream()
                .map(s -> s.split("#")[0])
                .anyMatch(s -> s.equals(tablePath.getObjectName()));
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException {
        return CatalogTableStatistics.UNKNOWN;
    }
}
