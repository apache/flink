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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Test Catalog for testing time travel. */
public class TestTimeTravelCatalog extends GenericInMemoryCatalog {

    private final Map<ObjectPath, List<Tuple2<Long, CatalogTable>>> timeTravelTables;

    public TestTimeTravelCatalog(String name) {
        super(name);

        this.timeTravelTables = new HashMap<>();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath, long timestamp)
            throws TableNotExistException {

        if (timeTravelTables.containsKey(tablePath)) {
            List<Tuple2<Long, CatalogTable>> tableList = timeTravelTables.get(tablePath);

            Optional<Tuple2<Long, CatalogTable>> table =
                    tableList.stream()
                            .filter(t -> t.f0 <= timestamp)
                            .max(Comparator.comparing(t -> t.f0));

            if (table.isPresent()) {
                return table.get().f1;
            }
        }

        return super.getTable(tablePath);
    }

    /**
     * @param tableName Table name
     * @param schema Table schema of the table
     * @param properties Table properties to construct a table instance
     * @param timestamp The snapshot of the table
     */
    public void registerTableForTimeTravel(
            String tableName, Schema schema, Map<String, String> properties, long timestamp)
            throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
        CatalogTable catalogTable =
                CatalogTable.of(schema, "", Collections.emptyList(), properties, timestamp);
        ObjectPath objectPath = new ObjectPath(getDefaultDatabase(), tableName);
        if (!timeTravelTables.containsKey(objectPath)) {
            timeTravelTables.put(objectPath, new ArrayList<>());
        }

        timeTravelTables.get(objectPath).add(Tuple2.of(timestamp, catalogTable));
        if (super.tableExists(objectPath)) {
            super.dropTable(objectPath, true);
        }
        // We need to register the newest version table to the parent so that we don't have to
        // implement other methods, such as getPartition or getTableStatistics, in this class.
        super.createTable(objectPath, catalogTable, true);
    }
}
